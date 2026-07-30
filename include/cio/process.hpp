// Child processes, awaited through the reactor.
//
//     cio::process::Command cmd("/bin/echo");
//     cmd.args = {"hello"};
//     cmd.stdout_pipe = true;
//     auto child = cio::process::spawn(cmd);
//     auto output = co_await cio::io::read_all(*child->out());
//     auto status = co_await child->wait();
//
// Waiting uses pidfd, so a child is a pollable descriptor like any other: the
// task suspends on the worker-local reactor rather than blocking a thread in
// waitpid or installing a SIGCHLD handler. That also removes the classic
// footgun — a SIGCHLD handler and a signal-based Notify both wanting the same
// signal.
//
// Requires Linux 5.3 for pidfd_open and 5.4 for waitid on a pidfd; spawn()
// reports ENOSYS on anything older rather than silently falling back to a
// blocking wait.
//
// OWNERSHIP: Child is move-only. Destroying it without wait() leaves the process
// running and reaped by init, which is what Go's os.Process does; call wait() to
// collect a status, or kill() first if the child should not outlive its parent.
#pragma once

#include <csignal>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include "cio/net.hpp"
#include "cio/result.hpp"
#include "cio/scoped.hpp"
#include "cio/task.hpp"

namespace cio::process {

// What a finished child reported.
struct Status {
    // The value passed to exit(), when the child exited normally.
    std::optional<int> exit_code;
    // The signal that killed it, when it did not.
    std::optional<int> signal;

    bool success() const noexcept { return exit_code.has_value() && *exit_code == 0; }
    bool signalled() const noexcept { return signal.has_value(); }
};

struct Command {
    explicit Command(std::string program_path) : program(std::move(program_path)) {}

    std::string program;
    // argv[0] is supplied automatically from `program` unless argv0 is set.
    std::vector<std::string> args;
    std::optional<std::string> argv0;

    // Inherited from the parent when empty.
    std::optional<std::vector<std::string>> env;
    std::optional<std::string> working_dir;

    // Each requested pipe becomes a socket on the Child. Streams not piped are
    // inherited from the parent.
    bool stdin_pipe = false;
    bool stdout_pipe = false;
    bool stderr_pipe = false;
};

// A running child process.
class Child {
public:
    Child() = default;
    ~Child();

    Child(Child&&) noexcept;
    Child& operator=(Child&&) noexcept;
    Child(const Child&) = delete;
    Child& operator=(const Child&) = delete;

    bool valid() const noexcept { return pid_ > 0; }
    int pid() const noexcept { return pid_; }

    // Pipes, present only when requested. `in()` is where the parent writes.
    PollableFd* in() noexcept { return stdin_ ? &*stdin_ : nullptr; }
    PollableFd* out() noexcept { return stdout_ ? &*stdout_ : nullptr; }
    PollableFd* err() noexcept { return stderr_ ? &*stderr_ : nullptr; }

    // Closes the child's stdin, which is how a filter is told there is no more
    // input. Without it a child reading to EOF never finishes.
    void close_in() { stdin_.reset(); }

    // Suspends until the child exits, then reaps it. Calling it twice returns
    // the same status rather than blocking forever on an already-reaped child.
    //
    // Cancellable and deadline-capable through the pidfd, so a caller can stop
    // waiting without leaving a thread parked.
    Task<Result<Status>> wait();

    void set_deadline(TimePoint deadline);
    void clear_deadline();
    void set_cancel(CancelToken token);
    void clear_cancel();

    // Sends a signal. Uses pidfd, so it cannot hit an unrelated process that
    // recycled the pid.
    Result<void> signal(int number) const;
    Result<void> kill() const { return signal(SIGKILL); }
    Result<void> interrupt() const { return signal(SIGINT); }
    Result<void> terminate() const { return signal(SIGTERM); }

private:
    friend Result<Child> spawn(const Command& command);

    int pid_ = -1;
    std::optional<PollableFd> pidfd_;
    std::optional<PollableFd> stdin_;
    std::optional<PollableFd> stdout_;
    std::optional<PollableFd> stderr_;
    std::optional<Status> status_;
};

// Starts `command`. Reports the failure the child would have had: a missing
// program is ENOENT here rather than a mysterious exit status later, because the
// exec result is reported back over a close-on-exec pipe.
Result<Child> spawn(const Command& command);

// Runs to completion and collects stdout and stderr. The convenience form, for
// the common case of shelling out and reading the result.
struct Output {
    Status status;
    std::vector<std::byte> out;
    std::vector<std::byte> err;
};

Task<Result<Output>> run(Command command,
                         std::size_t max_output = 16u * 1024 * 1024);

}  // namespace cio::process
