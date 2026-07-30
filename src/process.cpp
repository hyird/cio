#include "cio/process.hpp"

#include <fcntl.h>
#include <sys/syscall.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <utility>

#include "cio/io.hpp"
#include "cio/spawn.hpp"

namespace cio::process {
namespace {

int pidfd_open(int pid) noexcept {
    return static_cast<int>(::syscall(SYS_pidfd_open, pid, 0u));
}

int pidfd_send_signal(int pidfd, int signal) noexcept {
    return static_cast<int>(
        ::syscall(SYS_pidfd_send_signal, pidfd, signal, nullptr, 0u));
}

// A close-on-exec pipe pair. RAII because the child path must not leak a
// descriptor into the exec'd program.
class Pipe {
public:
    Pipe() = default;
    ~Pipe() {
        close_read();
        close_write();
    }
    Pipe(const Pipe&) = delete;
    Pipe& operator=(const Pipe&) = delete;

    Result<void> open() {
        int fds[2];
        if (::pipe2(fds, O_CLOEXEC) != 0) return Error::from_errno();
        read_ = fds[0];
        write_ = fds[1];
        return ok();
    }

    int read_end() const noexcept { return read_; }
    int write_end() const noexcept { return write_; }
    int take_read() noexcept { return std::exchange(read_, -1); }
    int take_write() noexcept { return std::exchange(write_, -1); }
    void close_read() noexcept {
        if (read_ >= 0) ::close(std::exchange(read_, -1));
    }
    void close_write() noexcept {
        if (write_ >= 0) ::close(std::exchange(write_, -1));
    }

private:
    int read_ = -1;
    int write_ = -1;
};

std::vector<char*> build_argv(const Command& command,
                              std::vector<std::string>& storage) {
    storage.clear();
    storage.push_back(command.argv0.value_or(command.program));
    for (const auto& arg : command.args) storage.push_back(arg);

    std::vector<char*> argv;
    argv.reserve(storage.size() + 1);
    for (auto& s : storage) argv.push_back(s.data());
    argv.push_back(nullptr);
    return argv;
}

// Moves `fd` onto `target`, clearing close-on-exec so it survives exec.
bool redirect(int fd, int target) noexcept {
    if (fd == target) {
        const int flags = ::fcntl(fd, F_GETFD);
        return flags >= 0 && ::fcntl(fd, F_SETFD, flags & ~FD_CLOEXEC) == 0;
    }
    return ::dup2(fd, target) >= 0;
}

}  // namespace

Child::~Child() {
    // Deliberately does not wait: a destructor cannot suspend, and blocking here
    // would stall a worker. An un-waited child is reparented and reaped by init,
    // which is what Go's os.Process does too.
}

Child::Child(Child&& other) noexcept
    : pid_(std::exchange(other.pid_, -1)),
      pidfd_(std::move(other.pidfd_)),
      stdin_(std::move(other.stdin_)),
      stdout_(std::move(other.stdout_)),
      stderr_(std::move(other.stderr_)),
      status_(std::move(other.status_)) {}

Child& Child::operator=(Child&& other) noexcept {
    if (this != &other) {
        pid_ = std::exchange(other.pid_, -1);
        pidfd_ = std::move(other.pidfd_);
        stdin_ = std::move(other.stdin_);
        stdout_ = std::move(other.stdout_);
        stderr_ = std::move(other.stderr_);
        status_ = std::move(other.status_);
    }
    return *this;
}

Result<void> Child::signal(int number) const {
    if (!pidfd_ || !pidfd_->valid()) return Error{ESRCH};
    // Through the pidfd, so a recycled pid cannot be signalled by mistake.
    if (pidfd_send_signal(pidfd_->native_handle(), number) != 0) {
        return Error::from_errno();
    }
    return ok();
}

void Child::set_deadline(TimePoint deadline) {
    if (pidfd_) pidfd_->set_deadline(deadline);
}

void Child::clear_deadline() {
    if (pidfd_) pidfd_->clear_deadline();
}

void Child::set_cancel(CancelToken token) {
    if (pidfd_) pidfd_->set_cancel(std::move(token));
}

void Child::clear_cancel() {
    if (pidfd_) pidfd_->clear_cancel();
}

Task<Result<Status>> Child::wait() {
    if (status_) co_return *status_;  // already reaped
    if (!pidfd_ || !pidfd_->valid()) co_return Error{ESRCH};

    // A pidfd becomes readable when the process exits, so this is an ordinary
    // reactor wait rather than a blocked thread in waitpid.
    if (auto ready = co_await pidfd_->readable(); !ready) {
        co_return ready.error();
    }

    siginfo_t info{};
    // WEXITED on the pidfd reaps the child; P_PIDFD is what makes this immune to
    // pid reuse.
    if (::waitid(static_cast<idtype_t>(P_PIDFD),
                 static_cast<id_t>(pidfd_->native_handle()), &info,
                 WEXITED) != 0) {
        co_return Error::from_errno();
    }

    Status status;
    switch (info.si_code) {
        case CLD_EXITED:
            status.exit_code = info.si_status;
            break;
        case CLD_KILLED:
        case CLD_DUMPED:
            status.signal = info.si_status;
            break;
        default:
            // Stopped or continued: not an exit, so report rather than invent a
            // status the caller would treat as termination.
            co_return Error{EAGAIN};
    }
    status_ = status;
    co_return status;
}

Result<Child> spawn(const Command& command) {
    if (command.program.empty()) return Error{EINVAL};

    Pipe in_pipe;
    Pipe out_pipe;
    Pipe err_pipe;
    if (command.stdin_pipe) {
        if (auto opened = in_pipe.open(); !opened) return opened.error();
    }
    if (command.stdout_pipe) {
        if (auto opened = out_pipe.open(); !opened) return opened.error();
    }
    if (command.stderr_pipe) {
        if (auto opened = err_pipe.open(); !opened) return opened.error();
    }

    // Carries the exec failure back to the parent. Close-on-exec, so a
    // successful exec closes it and the parent reads end-of-file.
    Pipe exec_status;
    if (auto opened = exec_status.open(); !opened) return opened.error();

    std::vector<std::string> argv_storage;
    std::vector<char*> argv = build_argv(command, argv_storage);

    std::vector<std::string> env_storage;
    std::vector<char*> envp;
    if (command.env) {
        env_storage = *command.env;
        envp.reserve(env_storage.size() + 1);
        for (auto& e : env_storage) envp.push_back(e.data());
        envp.push_back(nullptr);
    }

    const pid_t pid = ::fork();
    if (pid < 0) return Error::from_errno();

    if (pid == 0) {
        // Child. Only async-signal-safe work from here: everything is either a
        // syscall or already-prepared memory, and any failure is reported over
        // exec_status rather than by throwing or printing.
        exec_status.close_read();
        const int report = exec_status.write_end();

        auto fail = [report]() {
            const int error = errno;
            (void)::write(report, &error, sizeof(error));
            ::_exit(127);
        };

        if (command.stdin_pipe && !redirect(in_pipe.read_end(), STDIN_FILENO)) {
            fail();
        }
        if (command.stdout_pipe &&
            !redirect(out_pipe.write_end(), STDOUT_FILENO)) {
            fail();
        }
        if (command.stderr_pipe &&
            !redirect(err_pipe.write_end(), STDERR_FILENO)) {
            fail();
        }
        if (command.working_dir && ::chdir(command.working_dir->c_str()) != 0) {
            fail();
        }

        if (command.env) {
            ::execve(command.program.c_str(), argv.data(), envp.data());
        } else {
            ::execv(command.program.c_str(), argv.data());
        }
        fail();
        ::_exit(127);
    }

    // Parent.
    exec_status.close_write();
    in_pipe.close_read();
    out_pipe.close_write();
    err_pipe.close_write();

    const int pidfd = pidfd_open(static_cast<int>(pid));
    if (pidfd < 0) {
        const int error = errno;
        // Without a pidfd there is no non-blocking way to wait, so reap the child
        // and report rather than silently degrading to a blocking waitpid.
        int discarded = 0;
        (void)::waitpid(pid, &discarded, 0);
        return Error{error == ENOSYS ? ENOSYS : error};
    }

    // Did exec succeed? A successful exec closes the write end, so this reads 0.
    int child_errno = 0;
    ssize_t got = 0;
    do {
        got = ::read(exec_status.read_end(), &child_errno, sizeof(child_errno));
    } while (got < 0 && errno == EINTR);
    if (got == static_cast<ssize_t>(sizeof(child_errno))) {
        ::close(pidfd);
        int discarded = 0;
        (void)::waitpid(pid, &discarded, 0);
        return Error{child_errno};
    }

    Child child;
    child.pid_ = static_cast<int>(pid);

    auto adopted = PollableFd::adopt(pidfd, /*already_nonblocking=*/false);
    if (!adopted) {
        ::close(pidfd);
        int discarded = 0;
        (void)::waitpid(pid, &discarded, 0);
        return adopted.error();
    }
    child.pidfd_ = std::move(*adopted);

    auto attach = [](int fd, std::optional<PollableFd>& slot) -> Result<void> {
        auto owned = PollableFd::adopt(fd, /*already_nonblocking=*/false);
        if (!owned) return owned.error();
        slot = std::move(*owned);
        return ok();
    };
    if (command.stdin_pipe) {
        if (auto r = attach(in_pipe.take_write(), child.stdin_); !r) {
            return r.error();
        }
    }
    if (command.stdout_pipe) {
        if (auto r = attach(out_pipe.take_read(), child.stdout_); !r) {
            return r.error();
        }
    }
    if (command.stderr_pipe) {
        if (auto r = attach(err_pipe.take_read(), child.stderr_); !r) {
            return r.error();
        }
    }
    return child;
}

Task<Result<Output>> run(Command command, std::size_t max_output) {
    command.stdout_pipe = true;
    command.stderr_pipe = true;

    auto child = spawn(command);
    if (!child) co_return child.error();

    // Drain both streams concurrently. Reading one to completion first would
    // deadlock as soon as the child filled the other pipe's buffer.
    auto err_task = cio::spawn(
        [](PollableFd* stream,
           std::size_t limit) -> Task<Result<std::vector<std::byte>>> {
            if (stream == nullptr) co_return std::vector<std::byte>{};
            co_return co_await io::read_all(*stream, limit);
        }(child->err(), max_output));

    Result<std::vector<std::byte>> out = std::vector<std::byte>{};
    if (child->out() != nullptr) {
        out = co_await io::read_all(*child->out(), max_output);
    }
    auto err = co_await err_task;

    auto status = co_await child->wait();
    if (!status) co_return status.error();
    if (!out) co_return out.error();
    if (!err) co_return err.error();

    Output result;
    result.status = *status;
    result.out = std::move(*out);
    result.err = std::move(*err);
    co_return result;
}

}  // namespace cio::process
