// Shared pieces for the HTTP servers under test, so that the only thing
// differing between them is the runtime.
//
// The response is byte-identical across all three servers, and so is the
// request framing, because a difference in either would show up as a
// difference in the runtimes.
#pragma once

#include <cstddef>

namespace bench {

// TechEmpower's plaintext response, minus the Date header — every server would
// have to format it identically for the comparison to mean anything, and
// nothing here is measuring strftime.
inline constexpr char kResponse[] =
    "HTTP/1.1 200 OK\r\n"
    "Server: bench\r\n"
    "Content-Type: text/plain\r\n"
    "Content-Length: 13\r\n"
    "\r\n"
    "Hello, World!";
inline constexpr std::size_t kResponseLen = sizeof(kResponse) - 1;

// Counts complete request headers in a byte stream, carrying a partial match of
// the blank-line terminator across reads.
//
// Responding once per read() would be wrong in two directions: a request split
// across two packets would draw two responses, and two pipelined requests in
// one read would draw one. Neither happens often with wrk on loopback, which is
// exactly why it would be a bad bug to have — it would not show up as a failure,
// only as a number.
class RequestSplitter {
public:
    int feed(const char* data, std::size_t n) noexcept {
        static constexpr char kTerm[4] = {'\r', '\n', '\r', '\n'};
        int complete = 0;
        for (std::size_t i = 0; i < n; ++i) {
            if (data[i] == kTerm[matched_]) {
                if (++matched_ == 4) {
                    matched_ = 0;
                    ++complete;
                }
            } else {
                matched_ = data[i] == '\r' ? 1 : 0;
            }
        }
        return complete;
    }

private:
    int matched_ = 0;
};

}  // namespace bench
