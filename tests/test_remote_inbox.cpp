#include <atomic>
#include <cstdint>
#include <thread>
#include <vector>

#include "cio/detail/queue.hpp"
#include "test_util.hpp"

namespace {

void* item(std::uint64_t value) {
    return reinterpret_cast<void*>(static_cast<std::uintptr_t>(value + 1));
}

std::uint64_t value_of(void* pointer) {
    return static_cast<std::uint64_t>(
               reinterpret_cast<std::uintptr_t>(pointer)) -
           1;
}

void test_full_does_not_leave_a_reservation_hole() {
    cio::detail::BoundedMpscQueue<8> queue;

    for (std::uint64_t i = 0; i < 8; ++i) CIO_CHECK(queue.try_push(item(i)));
    CIO_CHECK(!queue.try_push(item(100)));

    for (std::uint64_t i = 0; i < 4; ++i) {
        void* pointer = queue.pop();
        CIO_CHECK(pointer != nullptr);
        CIO_CHECK_EQ(value_of(pointer), i);
    }
    for (std::uint64_t i = 8; i < 12; ++i) CIO_CHECK(queue.try_push(item(i)));

    for (std::uint64_t i = 4; i < 12; ++i) {
        void* pointer = queue.pop();
        CIO_CHECK(pointer != nullptr);
        CIO_CHECK_EQ(value_of(pointer), i);
    }
    CIO_CHECK(queue.pop() == nullptr);
    CIO_CHECK(queue.empty());
}

void test_many_producers_one_owner_wrap_exactly_once() {
#if defined(__SANITIZE_THREAD__)
    constexpr std::uint64_t kPerProducer = 5'000;
#else
    constexpr std::uint64_t kPerProducer = 50'000;
#endif
    constexpr std::uint64_t kProducers = 8;
    constexpr std::uint64_t kTotal = kProducers * kPerProducer;

    cio::detail::BoundedMpscQueue<64> queue;
    std::atomic<bool> start{false};
    std::vector<std::thread> producers;
    producers.reserve(kProducers);

    for (std::uint64_t producer = 0; producer < kProducers; ++producer) {
        producers.emplace_back([&, producer] {
            while (!start.load(std::memory_order_acquire)) std::this_thread::yield();
            const std::uint64_t base = producer * kPerProducer;
            for (std::uint64_t i = 0; i < kPerProducer; ++i) {
                while (!queue.try_push(item(base + i))) std::this_thread::yield();
            }
        });
    }

    std::vector<unsigned char> seen(kTotal, 0);
    start.store(true, std::memory_order_release);
    std::uint64_t consumed = 0;
    while (consumed < kTotal) {
        void* pointer = queue.pop();
        if (pointer == nullptr) {
            std::this_thread::yield();
            continue;
        }
        const std::uint64_t value = value_of(pointer);
        CIO_CHECK(value < kTotal);
        if (value < kTotal) {
            CIO_CHECK_EQ(seen[value], static_cast<unsigned char>(0));
            seen[value] = 1;
        }
        ++consumed;
    }

    for (auto& producer : producers) producer.join();
    for (unsigned char count : seen) CIO_CHECK_EQ(count, static_cast<unsigned char>(1));
    CIO_CHECK(queue.pop() == nullptr);
}

}  // namespace

int main() {
    RUN_TEST(test_full_does_not_leave_a_reservation_hole);
    RUN_TEST(test_many_producers_one_owner_wrap_exactly_once);
    return cio_test::summary();
}
