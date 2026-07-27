// CIO_MAIN — writing main as a coroutine body.
//
//     #include <cio/cio.hpp>
//
//     CIO_MAIN {
//         auto ch = cio::make_chan<int>(8);
//         cio::go(producer(ch));
//         while (auto v = co_await ch.recv()) std::printf("%d\n", *v);
//         co_return 0;
//     }
//
// The C++ standard forbids main itself from being a coroutine
// ([basic.start.main]: "The function main shall not be a coroutine"), so this
// cannot be literally true. What the macro does instead is declare your body as
// a Task<int> and emit the three-line real main that stands up a runtime and
// blocks on it — which is the whole of the difference.
//
// Use the explicit form when you need to configure the runtime:
//
//     int main() {
//         cio::Runtime runtime({.worker_threads = 4});
//         return runtime.block_on(app());
//     }
#pragma once

#include <cstddef>
#include <span>
#include <string_view>

#include "cio/runtime.hpp"
#include "cio/task.hpp"

namespace cio {

namespace detail {

inline int g_argc = 0;
inline char** g_argv = nullptr;

inline void set_args(int argc, char** argv) noexcept {
    g_argc = argc;
    g_argv = argv;
}

// Runs the root task on a default runtime, turning an escaping exception into a
// diagnostic and a non-zero exit rather than a bare std::terminate.
int run_main(Task<int> task) noexcept;

}  // namespace detail

// The process arguments, available anywhere under CIO_MAIN.
inline std::span<char* const> args() noexcept {
    return {detail::g_argv, static_cast<std::size_t>(detail::g_argc)};
}

inline std::string_view arg(std::size_t index) noexcept {
    return index < static_cast<std::size_t>(detail::g_argc) ? detail::g_argv[index]
                                                            : std::string_view{};
}

}  // namespace cio

// Body becomes a cio::Task<int>; finish it with `co_return 0;`.
#define CIO_MAIN                                     \
    static ::cio::Task<int> cio_main_body();         \
    int main(int argc, char** argv) {                \
        ::cio::detail::set_args(argc, argv);         \
        return ::cio::detail::run_main(cio_main_body()); \
    }                                                \
    static ::cio::Task<int> cio_main_body()

// Same, with argc/argv named as coroutine parameters.
#define CIO_MAIN_ARGS(argc_name, argv_name)                          \
    static ::cio::Task<int> cio_main_body(int, char**);              \
    int main(int argc, char** argv) {                                \
        ::cio::detail::set_args(argc, argv);                         \
        return ::cio::detail::run_main(cio_main_body(argc, argv));   \
    }                                                                \
    static ::cio::Task<int> cio_main_body(int argc_name, char** argv_name)
