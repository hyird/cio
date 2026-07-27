// Minimal test harness: no external dependency, so `cmake && ctest` works on a
// bare box.
#pragma once

#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <sstream>
#include <string>

namespace cio_test {

inline std::atomic<int>& failure_count() {
    static std::atomic<int> failures{0};
    return failures;
}

inline void report_failure(const char* file, int line, const std::string& message) {
    std::fprintf(stderr, "  FAIL %s:%d: %s\n", file, line, message.c_str());
    failure_count().fetch_add(1, std::memory_order_relaxed);
}

inline int summary() {
    const int failures = failure_count().load();
    if (failures == 0) {
        std::fprintf(stderr, "OK\n");
        return 0;
    }
    std::fprintf(stderr, "%d check(s) failed\n", failures);
    return 1;
}

}  // namespace cio_test

#define CIO_CHECK(cond)                                                       \
    do {                                                                      \
        if (!(cond)) {                                                        \
            ::cio_test::report_failure(__FILE__, __LINE__, "!(" #cond ")");    \
        }                                                                     \
    } while (0)

#define CIO_CHECK_EQ(a, b)                                                    \
    do {                                                                      \
        const auto cio_lhs_ = (a);                                            \
        const auto cio_rhs_ = (b);                                            \
        if (!(cio_lhs_ == cio_rhs_)) {                                        \
            std::ostringstream cio_msg_;                                      \
            cio_msg_ << #a " == " #b " (" << cio_lhs_ << " vs " << cio_rhs_   \
                     << ")";                                                  \
            ::cio_test::report_failure(__FILE__, __LINE__, cio_msg_.str());   \
        }                                                                     \
    } while (0)

#define RUN_TEST(fn)                                                          \
    do {                                                                      \
        std::fprintf(stderr, "- %s\n", #fn);                                  \
        fn();                                                                 \
    } while (0)
