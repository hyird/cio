// cio — goroutine-style concurrency for C++20.
//
//     cio::Task<> worker(cio::Chan<int> jobs, cio::Chan<int> results) {
//         while (auto job = co_await jobs.recv()) {
//             co_await results.send(*job * 2);
//         }
//     }
//
//     int main() {
//         return cio::run([]() -> cio::Task<int> {
//             auto jobs = cio::make_chan<int>(64);
//             auto results = cio::make_chan<int>(64);
//             for (int i = 0; i < 4; ++i) cio::go(worker(jobs, results));
//             ...
//         }());
//     }
//
// The public surface has no thread in it. Underneath: M:N scheduling with work
// stealing, an edge-triggered reactor, sharded timer heaps and a blocking pool.
#pragma once

#include "cio/blocking.hpp"
#include "cio/chan.hpp"
#include "cio/clock.hpp"
#include "cio/detail/metrics.hpp"
#include "cio/group.hpp"
#include "cio/main.hpp"
#include "cio/net.hpp"
#include "cio/result.hpp"
#include "cio/runtime.hpp"
#include "cio/select.hpp"
#include "cio/spawn.hpp"
#include "cio/sync.hpp"
#include "cio/task.hpp"
#include "cio/time.hpp"
