// The whole program, with main's body as a coroutine.
//
// CIO_MAIN declares this body as a cio::Task<int> and emits the real main that
// stands up the runtime — the standard forbids main itself from being a
// coroutine, so that wrapper is the irreducible difference from Go.
#include <cstdio>
#include <string>

#include "cio/cio.hpp"

using namespace std::chrono_literals;

cio::Task<> greeter(int id, cio::Chan<std::string> out) {
    co_await cio::sleep(std::chrono::milliseconds(10 * id));
    co_await out.send("hello from task " + std::to_string(id));
}

CIO_MAIN {
    std::printf("argv[0] = %.*s\n", static_cast<int>(cio::arg(0).size()), cio::arg(0).data());

    auto greetings = cio::make_chan<std::string>(4);

    cio::TaskGroup group;
    for (int i = 0; i < 4; ++i) group.spawn(greeter(i, greetings));

    for (int i = 0; i < 4; ++i) {
        auto line = co_await greetings.recv();
        std::printf("%s\n", line->c_str());
    }

    co_await group.join();
    co_return 0;
}
