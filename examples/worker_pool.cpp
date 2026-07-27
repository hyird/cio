// Fan-out / fan-in over channels, plus a cancellable worker loop.
//
// This is the Go worker-pool idiom transcribed one-to-one:
//   - a jobs channel the workers range over,
//   - a results channel they fan back into,
//   - a done channel (here, a CancelToken) selected against so a worker can be
//     stopped mid-flight.
#include <chrono>
#include <cstdio>
#include <string>

#include "cio/cio.hpp"

using namespace std::chrono_literals;

cio::Task<> worker(int id, cio::Chan<int> jobs, cio::Chan<std::string> results,
                   cio::CancelToken token) {
    for (;;) {
        auto sel = cio::select(cio::recv(jobs), cio::recv(token.done()));
        if (co_await sel == 1) {
            std::printf("worker %d: cancelled\n", id);
            co_return;
        }

        auto job = sel.get<0>();
        if (!job) co_return;  // jobs channel closed and drained

        // Pretend the work takes a while. Note this suspends the *task*; the
        // worker thread underneath goes and runs somebody else's job.
        co_await cio::sleep(std::chrono::milliseconds(10 + (*job % 7)));
        co_await results.send("job " + std::to_string(*job) + " -> worker " +
                              std::to_string(id));
    }
}

CIO_MAIN {
    auto jobs = cio::make_chan<int>(16);
    auto results = cio::make_chan<std::string>(16);
    cio::CancelSource stop;

    cio::TaskGroup workers;
    for (int i = 0; i < 4; ++i) {
        workers.spawn(worker(i, jobs, results, stop.token()));
    }

    // Feed jobs while draining results, so a slow consumer cannot deadlock the
    // producer against a full channel.
    cio::go([](cio::Chan<int> out) -> cio::Task<> {
        for (int i = 1; i <= 20; ++i) co_await out.send(i);
        out.close();
    }(jobs));

    for (int received = 0; received < 20; ++received) {
        auto line = co_await results.recv();
        if (!line) break;
        std::printf("%s\n", line->c_str());
    }

    co_await workers.join();
    std::printf("all workers done\n");
    co_return 0;
}
