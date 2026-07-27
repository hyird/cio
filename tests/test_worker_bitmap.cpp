#include "cio/detail/bitmap.hpp"
#include "test_util.hpp"

namespace {

void test_cross_word_set_find_clear() {
    cio::detail::AtomicWorkerBitmap bitmap(130);
    CIO_CHECK(!bitmap.any());

    bitmap.set(0);
    bitmap.set(64);
    bitmap.set(129);
    CIO_CHECK(bitmap.any());
    CIO_CHECK(bitmap.test(0));
    CIO_CHECK(bitmap.test(64));
    CIO_CHECK(bitmap.test(129));

    CIO_CHECK_EQ(bitmap.find_from(1), cio::detail::WorkerId{64});
    CIO_CHECK_EQ(bitmap.find_from(65), cio::detail::WorkerId{129});
    CIO_CHECK_EQ(bitmap.find_from(130), cio::detail::WorkerId{0});

    CIO_CHECK(bitmap.clear(64));
    CIO_CHECK(!bitmap.clear(64));
    CIO_CHECK_EQ(bitmap.find_from(1), cio::detail::WorkerId{129});
}

void test_claim_is_exactly_once() {
    cio::detail::AtomicWorkerBitmap bitmap(130);
    bitmap.set(2);
    bitmap.set(66);
    bitmap.set(129);

    CIO_CHECK_EQ(bitmap.claim_from(3), cio::detail::WorkerId{66});
    CIO_CHECK_EQ(bitmap.claim_from(67), cio::detail::WorkerId{129});
    CIO_CHECK_EQ(bitmap.claim_from(0), cio::detail::WorkerId{2});
    CIO_CHECK_EQ(bitmap.claim_from(0), cio::detail::kInvalidWorkerId);
    CIO_CHECK(!bitmap.any());
}

}  // namespace

int main() {
    RUN_TEST(test_cross_word_set_find_clear);
    RUN_TEST(test_claim_is_exactly_once);
    return cio_test::summary();
}
