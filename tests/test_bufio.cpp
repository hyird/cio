#include <algorithm>
#include <chrono>
#include <span>
#include <string>
#include <vector>

#include "cio/cio.hpp"
#include "test_util.hpp"

using namespace std::chrono_literals;
namespace net = cio::net;

namespace {

std::span<const std::byte> bytes_of(std::string_view s) {
    return {reinterpret_cast<const std::byte*>(s.data()), s.size()};
}

// Hands out one byte at a time, so buffering and reassembly are actually
// exercised rather than accidentally satisfied by a single large read.
struct DribbleSource {
    std::string data;
    std::size_t at = 0;
    std::size_t chunk = 1;
    int reads = 0;

    cio::Task<cio::Result<std::size_t>> read(std::span<std::byte> out) {
        ++reads;
        const std::size_t n = std::min({out.size(), data.size() - at, chunk});
        for (std::size_t i = 0; i < n; ++i) {
            out[i] = static_cast<std::byte>(data[at + i]);
        }
        at += n;
        co_return n;
    }
};

struct CountingSink {
    std::string seen;
    int writes = 0;

    cio::Task<cio::Result<std::size_t>> write(std::span<const std::byte> in) {
        ++writes;
        for (std::byte b : in) seen += static_cast<char>(b);
        co_return in.size();
    }
};

void test_read_line_reassembles() {
    auto body = []() -> cio::Task<bool> {
        DribbleSource source{"first\nsecond\r\nthird"};
        cio::bufio::Reader in(source, 64);

        auto first = co_await in.read_line();
        CIO_CHECK(first.has_value());
        CIO_CHECK_EQ(**first, std::string("first"));

        // CRLF is stripped as well as LF.
        auto second = co_await in.read_line();
        CIO_CHECK(second.has_value());
        CIO_CHECK_EQ(**second, std::string("second"));

        // A trailing fragment with no newline is returned, not dropped.
        auto third = co_await in.read_line();
        CIO_CHECK(third.has_value());
        CIO_CHECK_EQ(**third, std::string("third"));

        // Clean end of stream reports nothing rather than an error.
        auto done = co_await in.read_line();
        CIO_CHECK(done.has_value());
        CIO_CHECK(!done->has_value());
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

// The point of buffering: many small reads become few syscalls.
void test_buffering_reduces_reads() {
    auto body = []() -> cio::Task<bool> {
        const std::string payload(4096, 'x');

        DribbleSource unbuffered{payload, 0, 4096};
        std::vector<std::byte> sink(4096);
        auto direct = co_await cio::io::read_full(
            unbuffered, std::span<std::byte>{sink});
        CIO_CHECK(direct.has_value());

        // 1024-byte source chunks through a 4 KiB buffer: the reader should pull
        // in bulk, so byte-at-a-time consumption costs no extra reads.
        DribbleSource source{payload, 0, 1024};
        cio::bufio::Reader in(source, 4096);
        for (std::size_t i = 0; i < payload.size(); ++i) {
            std::byte one[1];
            auto n = co_await in.read(one);
            CIO_CHECK(n.has_value());
            CIO_CHECK_EQ(*n, std::size_t{1});
        }
        // Four source chunks, not 4096 reads.
        CIO_CHECK(source.reads <= 6);
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

// A request at least as large as the buffer must bypass it instead of copying
// twice.
void test_large_read_bypasses_the_buffer() {
    auto body = []() -> cio::Task<bool> {
        DribbleSource source{std::string(8192, 'y'), 0, 8192};
        cio::bufio::Reader in(source, 1024);

        std::vector<std::byte> out(4096);
        auto n = co_await in.read(out);
        CIO_CHECK(n.has_value());
        // Served by one direct read of the caller's span, not 1024 at a time.
        CIO_CHECK_EQ(*n, std::size_t{4096});
        CIO_CHECK_EQ(source.reads, 1);
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

void test_read_full_peek_and_discard() {
    auto body = []() -> cio::Task<bool> {
        DribbleSource source{"HEADERbodybody", 0, 3};
        cio::bufio::Reader in(source, 32);

        std::vector<std::byte> header(6);
        // Go composes: io.ReadFull over the bufio.Reader, no duplicate method.
        auto filled = co_await cio::io::read_full(in, std::span<std::byte>{header});
        CIO_CHECK(filled.has_value());
        CIO_CHECK_EQ(std::string(reinterpret_cast<const char*>(header.data()), 6),
                     std::string("HEADER"));

        // Go's Peek(n): a view of the next n bytes, filling as needed and
        // never consuming.
        auto peeked = co_await in.peek(4);
        CIO_CHECK(peeked.has_value());
        CIO_CHECK_EQ(peeked->size(), std::size_t{4});
        CIO_CHECK_EQ(static_cast<char>((*peeked)[0]), 'b');
        const std::size_t before = in.buffered();
        CIO_CHECK(before >= std::size_t{4});

        // A request beyond the buffer capacity is EMSGSIZE, as ErrBufferFull.
        auto too_wide = co_await in.peek(1024);
        CIO_CHECK(!too_wide.has_value());
        CIO_CHECK(too_wide.error().is(EMSGSIZE));

        // Go's Discard(n): skips, reading as needed; short only at EOF.
        auto dropped = co_await in.discard(1);
        CIO_CHECK(dropped.has_value());
        CIO_CHECK_EQ(*dropped, std::size_t{1});
        CIO_CHECK_EQ(in.buffered(), before - 1);
        auto drained = co_await in.discard(1000);
        CIO_CHECK(drained.has_value());
        CIO_CHECK(*drained < std::size_t{1000});  // hit end of stream
        CIO_CHECK_EQ(in.buffered(), std::size_t{0});

        // Asking for more than remains is Errc::closed, not a short success.
        std::vector<std::byte> too_much(64);
        auto truncated =
            co_await cio::io::read_full(in, std::span<std::byte>{too_much});
        CIO_CHECK(!truncated.has_value());
        CIO_CHECK(truncated.error().is(cio::Errc::closed));
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

// An unterminated stream must be a protocol error, not unbounded memory.
void test_line_limit_is_enforced() {
    auto body = []() -> cio::Task<bool> {
        DribbleSource source{std::string(5000, 'z'), 0, 500};
        cio::bufio::Reader in(source, 128);
        auto refused = co_await in.read_line(/*limit=*/1024);
        CIO_CHECK(!refused.has_value());
        CIO_CHECK(refused.error().is(EMSGSIZE));
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

void test_writer_batches_and_flushes() {
    auto body = []() -> cio::Task<bool> {
        CountingSink sink;
        {
            cio::bufio::Writer out(sink, 1024);
            for (int i = 0; i < 100; ++i) {
                CIO_CHECK((co_await out.write_string("ab")).has_value());
            }
            // Still held: nothing reached the sink yet.
            CIO_CHECK_EQ(sink.writes, 0);
            CIO_CHECK_EQ(out.buffered(), std::size_t{200});

            CIO_CHECK((co_await out.flush()).has_value());
            CIO_CHECK_EQ(sink.writes, 1);
            CIO_CHECK_EQ(sink.seen.size(), std::size_t{200});
            // Flushing twice is not an error and writes nothing more.
            CIO_CHECK((co_await out.flush()).has_value());
            CIO_CHECK_EQ(sink.writes, 1);
        }
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

void test_writer_flushes_when_full_and_bypasses_large() {
    auto body = []() -> cio::Task<bool> {
        CountingSink sink;
        cio::bufio::Writer out(sink, 64);

        // Exceeding capacity flushes what is held first.
        CIO_CHECK((co_await out.write_string(std::string(50, 'a'))).has_value());
        CIO_CHECK_EQ(sink.writes, 0);
        CIO_CHECK((co_await out.write_string(std::string(50, 'b'))).has_value());
        CIO_CHECK_EQ(sink.writes, 1);   // the first 50 went out
        CIO_CHECK_EQ(out.buffered(), std::size_t{50});

        // A write at least as large as the buffer goes straight through.
        CIO_CHECK((co_await out.write_string(std::string(200, 'c'))).has_value());
        CIO_CHECK_EQ(out.buffered(), std::size_t{0});
        CIO_CHECK((co_await out.flush()).has_value());

        CIO_CHECK_EQ(sink.seen.size(), std::size_t{300});
        // Order is preserved across the buffered and bypassed paths.
        CIO_CHECK_EQ(sink.seen.substr(0, 50), std::string(50, 'a'));
        CIO_CHECK_EQ(sink.seen.substr(50, 50), std::string(50, 'b'));
        CIO_CHECK_EQ(sink.seen.substr(100, 200), std::string(200, 'c'));
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

// Over a real socket, which is what this exists for.
void test_over_a_socket() {
    auto body = []() -> cio::Task<bool> {
        auto listener = net::TcpListener::listen(net::SocketAddr::loopback_v4(0));
        CIO_CHECK(listener.has_value());
        const auto addr = listener->addr().value();

        auto serving = cio::spawn([](net::TcpListener l) -> cio::Task<std::string> {
            auto conn = co_await l.accept();
            if (!conn) co_return std::string{};
            cio::bufio::Reader in(*conn);
            std::string joined;
            while (auto line = co_await in.read_line()) {
                if (!*line) break;
                joined += **line;
                joined += '|';
            }
            co_return joined;
        }(std::move(*listener)));

        auto client = co_await net::TcpConn::dial(addr);
        CIO_CHECK(client.has_value());
        {
            cio::bufio::Writer out(*client);
            CIO_CHECK((co_await out.write_string("one\ntwo\nthree\n")).has_value());
            CIO_CHECK((co_await out.flush()).has_value());
        }
        CIO_CHECK(client->close_write().has_value());

        CIO_CHECK_EQ(co_await serving, std::string("one|two|three|"));
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}


// ------------------------------------------------- io helpers ---

void test_read_all_and_limit() {
    auto body = []() -> cio::Task<bool> {
        DribbleSource source{std::string(5000, 'k'), 0, 250};
        auto all = co_await cio::io::read_all(source);
        CIO_CHECK(all.has_value());
        CIO_CHECK_EQ(all->size(), std::size_t{5000});

        // The bound reports rather than truncating, so a cut-off read cannot be
        // mistaken for a complete one.
        DribbleSource big{std::string(5000, 'k'), 0, 250};
        auto refused = co_await cio::io::read_all(big, /*limit=*/1000);
        CIO_CHECK(!refused.has_value());
        CIO_CHECK(refused.error().is(EMSGSIZE));
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

void test_limit_reader_stops_at_the_bound() {
    auto body = []() -> cio::Task<bool> {
        DribbleSource source{"HEADER" + std::string(100, 'b'), 0, 4};
        cio::io::LimitReader limited(source, 6);

        // The wrapped reader ends at the bound, so a parser that reads to "end
        // of stream" consumes exactly the slice it was given.
        auto slice = co_await cio::io::read_all(limited);
        CIO_CHECK(slice.has_value());
        CIO_CHECK_EQ(slice->size(), std::size_t{6});
        CIO_CHECK_EQ(std::string(reinterpret_cast<const char*>(slice->data()), 6),
                     std::string("HEADER"));
        CIO_CHECK_EQ(limited.remaining(), std::uint64_t{0});

        // The underlying stream is untouched beyond the bound and can continue.
        std::vector<std::byte> rest(4);
        auto more = co_await cio::io::read_full(source,
                                                std::span<std::byte>{rest});
        CIO_CHECK(more.has_value());
        CIO_CHECK_EQ(std::string(reinterpret_cast<const char*>(rest.data()), 4),
                     std::string("bbbb"));
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

void test_tee_reader_mirrors() {
    auto body = []() -> cio::Task<bool> {
        DribbleSource source{"mirror me", 0, 3};
        CountingSink mirror;
        cio::io::TeeReader tee(source, mirror);

        auto read = co_await cio::io::read_all(tee);
        CIO_CHECK(read.has_value());
        CIO_CHECK_EQ(std::string(reinterpret_cast<const char*>(read->data()),
                                 read->size()),
                     std::string("mirror me"));
        // Everything read was also written, in order.
        CIO_CHECK_EQ(mirror.seen, std::string("mirror me"));
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

// A mirror that fails must fail the read, not silently lose the copy.
void test_tee_reader_propagates_mirror_failure() {
    struct FailingSink {
        cio::Task<cio::Result<std::size_t>> write(std::span<const std::byte>) {
            co_return cio::Error{ENOSPC};
        }
    };
    auto body = []() -> cio::Task<bool> {
        DribbleSource source{"data", 0, 4};
        FailingSink mirror;
        cio::io::TeeReader tee(source, mirror);

        std::byte buffer[4];
        auto read = co_await tee.read(buffer);
        CIO_CHECK(!read.has_value());
        CIO_CHECK(read.error().is(ENOSPC));
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

}  // namespace

int main() {
    RUN_TEST(test_read_all_and_limit);
    RUN_TEST(test_limit_reader_stops_at_the_bound);
    RUN_TEST(test_tee_reader_mirrors);
    RUN_TEST(test_tee_reader_propagates_mirror_failure);
    RUN_TEST(test_read_line_reassembles);
    RUN_TEST(test_buffering_reduces_reads);
    RUN_TEST(test_large_read_bypasses_the_buffer);
    RUN_TEST(test_read_full_peek_and_discard);
    RUN_TEST(test_line_limit_is_enforced);
    RUN_TEST(test_writer_batches_and_flushes);
    RUN_TEST(test_writer_flushes_when_full_and_bypasses_large);
    RUN_TEST(test_over_a_socket);
    return cio_test::summary();
}
