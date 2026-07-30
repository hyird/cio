#include <signal.h>

#include <chrono>
#include <span>
#include <string>
#include <vector>

#include "cio/cio.hpp"
#include "test_util.hpp"

using namespace std::chrono_literals;
namespace process = cio::process;

namespace {

std::string string_of(std::span<const std::byte> b) {
    return std::string(reinterpret_cast<const char*>(b.data()), b.size());
}

std::span<const std::byte> bytes_of(std::string_view s) {
    return {reinterpret_cast<const std::byte*>(s.data()), s.size()};
}

// A pipe must work with the io helpers, which is what read/write on PollableFd
// buys; framing interfaces still use readable()/writable().
static_assert(cio::io::Reader<cio::PollableFd>);
static_assert(cio::io::Writer<cio::PollableFd>);

void test_run_collects_output() {
    auto body = []() -> cio::Task<bool> {
        process::Command cmd("/bin/echo");
        cmd.args = {"hello", "world"};
        auto result = co_await process::run(std::move(cmd));
        CIO_CHECK(result.has_value());
        CIO_CHECK(result->status.success());
        CIO_CHECK_EQ(result->status.exit_code.value_or(-1), 0);
        CIO_CHECK_EQ(string_of(result->out), std::string("hello world\n"));
        CIO_CHECK(result->err.empty());
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

void test_exit_code_and_stderr() {
    auto body = []() -> cio::Task<bool> {
        process::Command cmd("/bin/sh");
        cmd.args = {"-c", "echo out; echo err >&2; exit 3"};
        auto result = co_await process::run(std::move(cmd));
        CIO_CHECK(result.has_value());
        CIO_CHECK(!result->status.success());
        CIO_CHECK_EQ(result->status.exit_code.value_or(-1), 3);
        CIO_CHECK(!result->status.signalled());
        CIO_CHECK_EQ(string_of(result->out), std::string("out\n"));
        CIO_CHECK_EQ(string_of(result->err), std::string("err\n"));
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

// A missing program must be reported at spawn, not as a mysterious exit status:
// that is what the exec-status pipe is for.
void test_missing_program_reports_enoent() {
    process::Command cmd("/nonexistent/program/xyz");
    auto child = process::spawn(cmd);
    CIO_CHECK(!child.has_value());
    CIO_CHECK(child.error().is(ENOENT));

    process::Command empty("");
    CIO_CHECK(!process::spawn(empty).has_value());
}

void test_stdin_pipe_feeds_a_filter() {
    auto body = []() -> cio::Task<bool> {
        process::Command cmd("/bin/cat");
        cmd.stdin_pipe = true;
        cmd.stdout_pipe = true;

        auto child = process::spawn(cmd);
        CIO_CHECK(child.has_value());
        CIO_CHECK(child->valid());
        CIO_CHECK(child->pid() > 0);

        CIO_CHECK(child->in() != nullptr);
        CIO_CHECK((co_await child->in()->write_all(bytes_of("piped input")))
                      .has_value());
        // Without closing stdin, a filter reading to EOF never finishes.
        child->close_in();

        auto out = co_await cio::io::read_all(*child->out());
        CIO_CHECK(out.has_value());
        CIO_CHECK_EQ(string_of(*out), std::string("piped input"));

        auto status = co_await child->wait();
        CIO_CHECK(status.has_value());
        CIO_CHECK(status->success());
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

void test_kill_reports_signal() {
    auto body = []() -> cio::Task<bool> {
        process::Command cmd("/bin/sleep");
        cmd.args = {"60"};
        auto child = process::spawn(cmd);
        CIO_CHECK(child.has_value());

        CIO_CHECK(child->kill().has_value());
        auto status = co_await child->wait();
        CIO_CHECK(status.has_value());
        CIO_CHECK(status->signalled());
        CIO_CHECK_EQ(status->signal.value_or(0), SIGKILL);
        CIO_CHECK(!status->success());

        // wait() again returns the same status rather than blocking on an
        // already-reaped child.
        auto again = co_await child->wait();
        CIO_CHECK(again.has_value());
        CIO_CHECK(again->signalled());
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

// Waiting goes through the reactor, so a deadline interrupts it without leaving
// a thread parked in waitpid.
void test_wait_honours_a_deadline() {
    auto body = []() -> cio::Task<bool> {
        process::Command cmd("/bin/sleep");
        cmd.args = {"60"};
        auto child = process::spawn(cmd);
        CIO_CHECK(child.has_value());

        child->set_deadline(cio::Clock::now() + 30ms);
        const auto started = cio::Clock::now();
        auto timed_out = co_await child->wait();
        const auto elapsed = cio::Clock::now() - started;
        CIO_CHECK(!timed_out.has_value());
        CIO_CHECK(timed_out.error().is_timeout());
        CIO_CHECK(elapsed < 5s);

        // The child is still running; clean it up.
        child->clear_deadline();
        CIO_CHECK(child->kill().has_value());
        auto status = co_await child->wait();
        CIO_CHECK(status.has_value());
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

void test_wait_is_cancellable() {
    auto body = []() -> cio::Task<bool> {
        process::Command cmd("/bin/sleep");
        cmd.args = {"60"};
        auto child = process::spawn(cmd);
        CIO_CHECK(child.has_value());

        cio::CancelSource stop;
        child->set_cancel(stop.token());
        auto canceller = cio::spawn([](cio::CancelSource s) -> cio::Task<> {
            co_await cio::sleep(30ms);
            s.cancel();
        }(stop));

        auto cancelled = co_await child->wait();
        co_await canceller;
        CIO_CHECK(!cancelled.has_value());
        CIO_CHECK(cancelled.error().is_cancelled());

        child->clear_cancel();
        CIO_CHECK(child->kill().has_value());
        (void)co_await child->wait();
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

void test_env_and_working_dir() {
    auto body = []() -> cio::Task<bool> {
        process::Command cmd("/bin/sh");
        cmd.args = {"-c", "echo $CIO_TEST_VAR; pwd"};
        cmd.env = std::vector<std::string>{"CIO_TEST_VAR=set-by-test", "PATH=/bin"};
        cmd.working_dir = "/tmp";

        auto result = co_await process::run(std::move(cmd));
        CIO_CHECK(result.has_value());
        CIO_CHECK(result->status.success());
        const std::string out = string_of(result->out);
        CIO_CHECK(out.find("set-by-test") != std::string::npos);
        CIO_CHECK(out.find("/tmp") != std::string::npos);
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

// Both streams must drain concurrently: reading one to completion first
// deadlocks as soon as the child fills the other pipe.
void test_large_output_on_both_streams() {
    auto body = []() -> cio::Task<bool> {
        process::Command cmd("/bin/sh");
        cmd.args = {"-c",
                    "for i in $(seq 1 400); do "
                    "echo aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa; "
                    "echo bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb >&2; done"};
        auto result = co_await process::run(std::move(cmd));
        CIO_CHECK(result.has_value());
        CIO_CHECK(result->status.success());
        CIO_CHECK(result->out.size() > std::size_t{16000});
        CIO_CHECK(result->err.size() > std::size_t{16000});
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

// A pipe should work through bufio, since PollableFd satisfies the concepts.
void test_pipe_through_bufio() {
    auto body = []() -> cio::Task<bool> {
        process::Command cmd("/bin/sh");
        cmd.args = {"-c", "echo one; echo two; echo three"};
        cmd.stdout_pipe = true;

        auto child = process::spawn(cmd);
        CIO_CHECK(child.has_value());

        cio::bufio::Reader in(*child->out());
        std::string joined;
        while (auto line = co_await in.read_line()) {
            if (!*line) break;
            joined += **line;
            joined += '|';
        }
        CIO_CHECK_EQ(joined, std::string("one|two|three|"));
        CIO_CHECK((co_await child->wait())->success());
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

}  // namespace

int main() {
    RUN_TEST(test_run_collects_output);
    RUN_TEST(test_exit_code_and_stderr);
    RUN_TEST(test_missing_program_reports_enoent);
    RUN_TEST(test_stdin_pipe_feeds_a_filter);
    RUN_TEST(test_kill_reports_signal);
    RUN_TEST(test_wait_honours_a_deadline);
    RUN_TEST(test_wait_is_cancellable);
    RUN_TEST(test_env_and_working_dir);
    RUN_TEST(test_large_output_on_both_streams);
    RUN_TEST(test_pipe_through_bufio);
    return cio_test::summary();
}
