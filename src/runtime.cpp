#include "cio/runtime.hpp"

#include <cstdio>
#include <cstdlib>

#include "cio/main.hpp"
#include "cio/result.hpp"

namespace cio {

Runtime::Runtime(RuntimeOptions options)
    : sched_(std::make_unique<detail::Scheduler>(options.worker_threads,
                                                 options.max_blocking_threads)) {
    detail::set_default_scheduler(sched_.get());
    sched_->start();
}

Runtime::~Runtime() {
    if (sched_) sched_->shutdown();
}

void Runtime::shutdown() {
    if (sched_) sched_->shutdown();
}

namespace detail {

int run_main(Task<int> task) noexcept {
    try {
        Runtime runtime;
        return runtime.block_on(std::move(task));
    } catch (const std::exception& e) {
        std::fprintf(stderr, "cio: unhandled exception escaped main: %s\n", e.what());
        return 1;
    } catch (...) {
        std::fprintf(stderr, "cio: unhandled exception escaped main\n");
        return 1;
    }
}

[[noreturn]] void abort_on_unhandled_exception(std::exception_ptr e) noexcept {
    const char* what = "unknown exception";
    try {
        std::rethrow_exception(e);
    } catch (const std::exception& ex) {
        what = ex.what();
    } catch (...) {
    }
    std::fprintf(stderr, "cio: unhandled exception escaped a detached task: %s\n", what);
    std::fflush(stderr);
    std::abort();
}

[[noreturn]] void throw_system_error(Error e) { throw SystemError(e); }

}  // namespace detail
}  // namespace cio
