#include "cio/signal.hpp"

#include <signal.h>
#include <sys/signalfd.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>

namespace cio::signal {
namespace {

Result<sigset_t> make_set(const std::vector<int>& signals) {
    sigset_t mask;
    if (::sigemptyset(&mask) != 0) return Error::from_errno();
    for (const int number : signals) {
        if (::sigaddset(&mask, number) != 0) return Error::from_errno();
    }
    return mask;
}

}  // namespace

Result<void> block(const std::vector<int>& signals) {
    auto mask = make_set(signals);
    if (!mask) return mask.error();

    // pthread_sigmask returns its error rather than setting errno.
    const int rc = ::pthread_sigmask(SIG_BLOCK, &*mask, nullptr);
    if (rc != 0) return Error{rc};
    return ok();
}

Result<void> block(std::initializer_list<int> signals) {
    return block(std::vector<int>(signals));
}

Result<SignalSet> SignalSet::subscribe(const std::vector<int>& signals) {
    if (signals.empty()) return Error{EINVAL};

    auto mask = make_set(signals);
    if (!mask) return mask.error();

    // A signal that is not blocked runs its default disposition instead of
    // reaching the descriptor, which would silently never deliver. Report that
    // as a contract violation rather than creating a set that cannot fire.
    sigset_t current;
    const int rc = ::pthread_sigmask(SIG_BLOCK, nullptr, &current);
    if (rc != 0) return Error{rc};
    for (const int number : signals) {
        const int member = ::sigismember(&current, number);
        if (member < 0) return Error::from_errno();
        if (member == 0) return Error{Errc::broken};
    }

    const int fd = ::signalfd(-1, &*mask, SFD_NONBLOCK | SFD_CLOEXEC);
    if (fd < 0) return Error::from_errno();

    SignalSet set;
    if (auto adopted = set.socket_.adopt_signalfd(fd); !adopted) {
        return adopted.error();
    }
    return set;
}

Result<SignalSet> SignalSet::subscribe(std::initializer_list<int> signals) {
    return subscribe(std::vector<int>(signals));
}

Task<Result<int>> SignalSet::recv() {
    if (!socket_.valid()) co_return Error{EBADF};

    for (;;) {
        signalfd_siginfo info{};
        const ssize_t n =
            ::read(socket_.native_handle(), &info, sizeof(info));
        if (n == static_cast<ssize_t>(sizeof(info))) {
            co_return static_cast<int>(info.ssi_signo);
        }
        if (n >= 0) co_return Error{EIO};

        const int error = errno;
        if (error != EAGAIN && error != EWOULDBLOCK && error != EINTR) {
            co_return Error{error};
        }
        if (error != EINTR) {
            // Park until the reactor reports readability; close(), a deadline
            // and cancellation all surface through this awaiter exactly as they
            // do for a socket read.
            if (auto ready = co_await socket_.readable(); !ready) {
                co_return ready.error();
            }
        }
    }
}

}  // namespace cio::signal
