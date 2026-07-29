#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <span>
#include <string>
#include <thread>
#include <vector>

#include "cio/cio.hpp"
#include "test_util.hpp"

using namespace std::chrono_literals;
namespace fs = cio::fs;

namespace {

std::span<const std::byte> bytes_of(std::string_view s) {
    return {reinterpret_cast<const std::byte*>(s.data()), s.size()};
}

std::string string_of(std::span<const std::byte> b) {
    return std::string(reinterpret_cast<const char*>(b.data()), b.size());
}

// A unique path under TMPDIR that is removed when the guard dies, so a failing
// assertion cannot leave files behind for the next run to trip over.
class TempPath {
public:
    TempPath() {
        const char* base = std::getenv("TMPDIR");
        path_ = std::string(base != nullptr ? base : "/tmp") +
                "/cio_test_fs_" + std::to_string(::getpid()) + "_" +
                std::to_string(counter_.fetch_add(1));
    }
    ~TempPath() { ::unlink(path_.c_str()); }

    TempPath(const TempPath&) = delete;
    TempPath& operator=(const TempPath&) = delete;

    const std::string& get() const noexcept { return path_; }

private:
    std::string path_;
    static std::atomic<unsigned> counter_;
};

std::atomic<unsigned> TempPath::counter_{0};

void test_create_write_read_roundtrip() {
    TempPath path;
    auto body = [&path]() -> cio::Task<bool> {
        auto file = co_await fs::create(path.get());
        CIO_CHECK(file.has_value());
        CIO_CHECK(file->valid());

        auto written = co_await file->write_all(bytes_of("hello file"));
        CIO_CHECK(written.has_value());
        auto synced = co_await file->sync();
        CIO_CHECK(synced.has_value());

        auto info = co_await file->stat();
        CIO_CHECK(info.has_value());
        CIO_CHECK_EQ(info->size, std::uint64_t{10});
        CIO_CHECK(info->is_regular());
        CIO_CHECK(!info->is_directory());

        // Positioned reads do not disturb the shared offset.
        std::vector<std::byte> buffer(10);
        auto n = co_await file->read_at(buffer, 0);
        CIO_CHECK(n.has_value());
        CIO_CHECK_EQ(*n, std::size_t{10});
        CIO_CHECK_EQ(string_of(buffer), std::string("hello file"));

        auto tail = co_await file->read_at(std::span<std::byte>{buffer}.first(4), 6);
        CIO_CHECK(tail.has_value());
        CIO_CHECK_EQ(*tail, std::size_t{4});
        CIO_CHECK_EQ(string_of(std::span<const std::byte>{buffer}.first(4)),
                     std::string("file"));

        // Reading past the end returns zero, not an error.
        auto eof = co_await file->read_at(buffer, 100);
        CIO_CHECK(eof.has_value());
        CIO_CHECK_EQ(*eof, std::size_t{0});
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

void test_sequential_offset_and_seek() {
    TempPath path;
    auto body = [&path]() -> cio::Task<bool> {
        auto file = co_await fs::create(path.get());
        CIO_CHECK(file.has_value());
        CIO_CHECK((co_await file->write_all(bytes_of("abcdefgh"))).has_value());

        // The shared offset sits at the end after writing.
        auto rewound = co_await file->seek(0);
        CIO_CHECK(rewound.has_value());
        CIO_CHECK_EQ(*rewound, std::uint64_t{0});

        std::vector<std::byte> first(3);
        auto a = co_await file->read(first);
        CIO_CHECK(a.has_value());
        CIO_CHECK_EQ(string_of(first), std::string("abc"));

        // A second read continues from where the first stopped.
        std::vector<std::byte> second(3);
        auto b = co_await file->read(second);
        CIO_CHECK(b.has_value());
        CIO_CHECK_EQ(string_of(second), std::string("def"));

        auto truncated = co_await file->truncate(4);
        CIO_CHECK(truncated.has_value());
        auto info = co_await file->stat();
        CIO_CHECK(info.has_value());
        CIO_CHECK_EQ(info->size, std::uint64_t{4});
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

void test_open_flags_and_errors() {
    TempPath path;
    auto body = [&path]() -> cio::Task<bool> {
        // Opening a missing file fails rather than creating it.
        auto missing = co_await fs::open(path.get());
        CIO_CHECK(!missing.has_value());
        CIO_CHECK(missing.error().is(ENOENT));

        auto stat_missing = co_await fs::stat(path.get());
        CIO_CHECK(!stat_missing.has_value());

        fs::OpenOptions options;
        options.access = fs::Access::read_write;
        options.create = true;
        options.exclusive = true;
        auto created = co_await fs::open(path.get(), options);
        CIO_CHECK(created.has_value());
        CIO_CHECK((co_await created->write_all(bytes_of("x"))).has_value());
        created->close();
        // close() is idempotent.
        created->close();
        CIO_CHECK(!created->valid());

        // O_EXCL rejects the second creation.
        auto again = co_await fs::open(path.get(), options);
        CIO_CHECK(!again.has_value());
        CIO_CHECK(again.error().is(EEXIST));

        // Operations on a closed handle report EBADF instead of touching a
        // descriptor the process may have reused.
        auto closed_read = co_await created->stat();
        CIO_CHECK(!closed_read.has_value());
        CIO_CHECK(closed_read.error().is(EBADF));

        // Append positions every write at the end regardless of the offset.
        fs::OpenOptions append;
        append.access = fs::Access::write_only;
        append.append = true;
        auto appender = co_await fs::open(path.get(), append);
        CIO_CHECK(appender.has_value());
        CIO_CHECK((co_await appender->write_all(bytes_of("yz"))).has_value());

        auto info = co_await fs::stat(path.get());
        CIO_CHECK(info.has_value());
        CIO_CHECK_EQ(info->size, std::uint64_t{3});
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

void test_remove_and_move_semantics() {
    TempPath path;
    auto body = [&path]() -> cio::Task<bool> {
        auto file = co_await fs::create(path.get());
        CIO_CHECK(file.has_value());
        const int fd = file->native_handle();

        fs::File moved = std::move(*file);
        CIO_CHECK(moved.valid());
        CIO_CHECK_EQ(moved.native_handle(), fd);
        // The moved-from handle owns nothing and must not close fd.
        CIO_CHECK(!file->valid());

        CIO_CHECK((co_await moved.write_all(bytes_of("gone soon"))).has_value());
        moved.close();

        auto removed = co_await fs::remove(path.get());
        CIO_CHECK(removed.has_value());
        auto after = co_await fs::stat(path.get());
        CIO_CHECK(!after.has_value());
        CIO_CHECK(after.error().is(ENOENT));
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

// Positioned reads are documented as safe to run concurrently when their
// buffers are distinct.
void test_concurrent_positioned_reads() {
    TempPath path;
    auto body = [&path]() -> cio::Task<bool> {
        const std::string payload(64 * 1024, 'q');
        auto writer = co_await fs::create(path.get());
        CIO_CHECK(writer.has_value());
        CIO_CHECK((co_await writer->write_all(bytes_of(payload))).has_value());
        writer->close();

        auto file = co_await fs::open(path.get());
        CIO_CHECK(file.has_value());

        cio::TaskGroup group;
        auto results = cio::make_chan<std::size_t>(16);
        for (int i = 0; i < 16; ++i) {
            group.spawn([](fs::File& target, cio::Chan<std::size_t> out,
                           int index) -> cio::Task<> {
                std::vector<std::byte> buffer(1024);
                auto n = co_await target.read_at(
                    buffer, static_cast<std::uint64_t>(index) * 1024);
                co_await out.send(n ? *n : 0);
            }(*file, results, i));
        }
        co_await group.join();

        std::size_t total = 0;
        for (int i = 0; i < 16; ++i) total += *co_await results.recv();
        CIO_CHECK_EQ(total, std::size_t{16 * 1024});
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

// The file class must bound how many file operations run at once, and a task
// waiting for admission must not occupy a pool thread.
void test_file_admission_is_bounded() {
    TempPath path;
    auto body = [&path]() -> cio::Task<bool> {
        auto writer = co_await fs::create(path.get());
        CIO_CHECK(writer.has_value());
        CIO_CHECK((co_await writer->write_all(bytes_of("payload"))).has_value());
        writer->close();

        auto file = co_await fs::open(path.get());
        CIO_CHECK(file.has_value());

        cio::TaskGroup group;
        auto done = cio::make_chan<bool>(64);
        for (int i = 0; i < 64; ++i) {
            group.spawn([](fs::File& target, cio::Chan<bool> out) -> cio::Task<> {
                std::vector<std::byte> buffer(8);
                auto n = co_await target.read_at(buffer, 0);
                co_await out.send(n.has_value());
            }(*file, done));
        }

        // Far more operations than the admission limit; all must still finish.
        co_await group.join();
        int succeeded = 0;
        for (int i = 0; i < 64; ++i) {
            if (*co_await done.recv()) ++succeeded;
        }
        CIO_CHECK_EQ(succeeded, 64);
        co_return true;
    };

    cio::RuntimeOptions options;
    options.max_file_operations = 4;
    cio::Runtime runtime(options);
    CIO_CHECK(runtime.block_on(body()));
}

// A saturated file class must not stop name resolution from making progress:
// that is the whole point of separate wait queues.
void test_file_saturation_does_not_starve_resolver() {
    TempPath path;
    auto body = [&path]() -> cio::Task<bool> {
        auto writer = co_await fs::create(path.get());
        CIO_CHECK(writer.has_value());
        CIO_CHECK((co_await writer->write_all(bytes_of("payload"))).has_value());
        writer->close();

        auto file = co_await fs::open(path.get());
        CIO_CHECK(file.has_value());

        // Occupy the file class well beyond its limit.
        cio::TaskGroup files;
        for (int i = 0; i < 48; ++i) {
            files.spawn([](fs::File& target) -> cio::Task<> {
                std::vector<std::byte> buffer(8);
                (void)co_await target.read_at(buffer, 0);
            }(*file));
        }

        // The resolver has its own admission slots, so this completes rather
        // than queueing behind all 48 file reads.
        auto addresses = co_await cio::net::resolve("localhost", 80);
        CIO_CHECK(addresses.has_value());

        co_await files.join();
        co_return true;
    };

    cio::RuntimeOptions options;
    options.max_file_operations = 2;
    options.max_resolver_operations = 4;
    cio::Runtime runtime(options);
    CIO_CHECK(runtime.block_on(body()));
}

}  // namespace

int main() {
    RUN_TEST(test_create_write_read_roundtrip);
    RUN_TEST(test_sequential_offset_and_seek);
    RUN_TEST(test_open_flags_and_errors);
    RUN_TEST(test_remove_and_move_semantics);
    RUN_TEST(test_concurrent_positioned_reads);
    RUN_TEST(test_file_admission_is_bounded);
    RUN_TEST(test_file_saturation_does_not_starve_resolver);
    return cio_test::summary();
}
