#include <arpa/inet.h>
#include <netinet/in.h>

#include <chrono>
#include <cstring>
#include <span>
#include <string>
#include <vector>

#include "cio/cio.hpp"
#include "test_util.hpp"

using namespace std::chrono_literals;
namespace net = cio::net;
namespace dns = cio::dns;

namespace {

std::uint16_t be16(std::span<const std::byte> m, std::size_t at) {
    return static_cast<std::uint16_t>(
        (static_cast<unsigned>(std::to_integer<unsigned char>(m[at])) << 8) |
        static_cast<unsigned>(std::to_integer<unsigned char>(m[at + 1])));
}

void put16(std::vector<std::byte>& out, std::uint16_t v) {
    out.push_back(static_cast<std::byte>(v >> 8));
    out.push_back(static_cast<std::byte>(v & 0xFF));
}

// ------------------------------------------------------------- codec ---

void test_query_encoding() {
    auto query = dns::detail::build_query("example.com", dns::RecordType::a,
                                          0xBEEF);
    CIO_CHECK(query.has_value());
    const std::span<const std::byte> m{query->data(), query->size()};

    CIO_CHECK_EQ(be16(m, 0), std::uint16_t{0xBEEF});
    CIO_CHECK_EQ(be16(m, 2), std::uint16_t{0x0100});  // RD set
    CIO_CHECK_EQ(be16(m, 4), std::uint16_t{1});       // one question
    CIO_CHECK_EQ(be16(m, 6), std::uint16_t{0});       // no answers

    // Built explicitly: "\x07example" would parse as one 0x7E byte followed by
    // "xample", because a hex escape consumes every hex digit after it.
    std::string expected;
    expected += char{7};
    expected += "example";
    expected += char{3};
    expected += "com";
    CIO_CHECK_EQ(query->size(), std::size_t{12 + expected.size() + 1 + 4});
    CIO_CHECK_EQ(std::memcmp(query->data() + 12, expected.data(),
                             expected.size()),
                 0);
    CIO_CHECK_EQ(be16(m, m.size() - 4),
                 static_cast<std::uint16_t>(dns::RecordType::a));
    CIO_CHECK_EQ(be16(m, m.size() - 2), std::uint16_t{1});  // IN

    // A trailing root dot is accepted and means the same thing.
    auto rooted = dns::detail::build_query("example.com.", dns::RecordType::a, 1);
    CIO_CHECK(rooted.has_value());
    CIO_CHECK_EQ(rooted->size(), query->size());
}

void test_query_encoding_rejects_bad_names() {
    CIO_CHECK(!dns::detail::build_query("", dns::RecordType::a, 1).has_value());
    CIO_CHECK(!dns::detail::build_query(".", dns::RecordType::a, 1).has_value());
    // An empty label.
    CIO_CHECK(
        !dns::detail::build_query("a..b", dns::RecordType::a, 1).has_value());
    // A label over 63 bytes.
    CIO_CHECK(!dns::detail::build_query(std::string(64, 'a'),
                                        dns::RecordType::a, 1)
                   .has_value());
    // A name over 253 bytes.
    CIO_CHECK(!dns::detail::build_query(std::string(300, 'a'),
                                        dns::RecordType::a, 1)
                   .has_value());
}

// Builds a response for "example.com" carrying the given A records.
std::vector<std::byte> make_response(std::uint16_t id,
                                     const std::vector<std::string>& ipv4,
                                     std::uint16_t flags = 0x8180,
                                     bool compressed_answer_name = true) {
    std::vector<std::byte> m;
    put16(m, id);
    put16(m, flags);
    put16(m, 1);
    put16(m, static_cast<std::uint16_t>(ipv4.size()));
    put16(m, 0);
    put16(m, 0);

    std::string qname;
    qname += char{7};
    qname += "example";
    qname += char{3};
    qname += "com";
    for (char c : qname) m.push_back(static_cast<std::byte>(c));
    m.push_back(std::byte{0});
    put16(m, 1);  // A
    put16(m, 1);  // IN

    for (const auto& ip : ipv4) {
        if (compressed_answer_name) {
            // Pointer to offset 12, the question's name.
            m.push_back(static_cast<std::byte>(0xC0));
            m.push_back(static_cast<std::byte>(12));
        } else {
            for (char c : qname) m.push_back(static_cast<std::byte>(c));
            m.push_back(std::byte{0});
        }
        put16(m, 1);  // A
        put16(m, 1);  // IN
        put16(m, 0);
        put16(m, 60);  // TTL
        put16(m, 4);   // rdlength
        in_addr addr{};
        ::inet_pton(AF_INET, ip.c_str(), &addr);
        const auto* raw = reinterpret_cast<const std::byte*>(&addr);
        m.insert(m.end(), raw, raw + 4);
    }
    return m;
}

void test_response_parsing() {
    auto message = make_response(0x1234, {"93.184.216.34", "1.2.3.4"});
    auto answer = dns::detail::parse_response(message, 0x1234, 443);
    CIO_CHECK(answer.has_value());
    CIO_CHECK(!answer->truncated);
    CIO_CHECK_EQ(answer->addresses.size(), std::size_t{2});
    CIO_CHECK_EQ(answer->addresses[0].to_string(),
                 std::string("93.184.216.34:443"));
    CIO_CHECK_EQ(answer->addresses[1].to_string(), std::string("1.2.3.4:443"));

    // An uncompressed answer name must parse identically.
    auto uncompressed =
        make_response(0x1234, {"5.6.7.8"}, 0x8180, /*compressed=*/false);
    auto plain = dns::detail::parse_response(uncompressed, 0x1234, 80);
    CIO_CHECK(plain.has_value());
    CIO_CHECK_EQ(plain->addresses.size(), std::size_t{1});
    CIO_CHECK_EQ(plain->addresses[0].to_string(), std::string("5.6.7.8:80"));
}

void test_response_parsing_rejects_bad_messages() {
    auto message = make_response(0x1234, {"1.2.3.4"});

    // Wrong transaction id.
    CIO_CHECK(!dns::detail::parse_response(message, 0x9999, 53).has_value());

    // Truncated header.
    CIO_CHECK(!dns::detail::parse_response(
                   std::span<const std::byte>{message.data(), 4}, 0x1234, 53)
                   .has_value());

    // Cut mid-record: the length fields must be bounds-checked, not trusted.
    for (std::size_t cut = 12; cut < message.size(); cut += 3) {
        auto partial =
            dns::detail::parse_response(
                std::span<const std::byte>{message.data(), cut}, 0x1234, 53);
        // Either a clean error or a short address list; never a crash or an
        // address read past the end.
        if (partial) CIO_CHECK(partial->addresses.size() <= 1);
    }

    // NXDOMAIN maps to ENOENT rather than a generic parse failure.
    auto nxdomain = make_response(0x1234, {}, 0x8183);
    auto missing = dns::detail::parse_response(nxdomain, 0x1234, 53);
    CIO_CHECK(!missing.has_value());
    CIO_CHECK(missing.error().is(ENOENT));

    // The truncation bit survives to the caller.
    auto truncated = make_response(0x1234, {"1.2.3.4"}, 0x8380);
    auto tc = dns::detail::parse_response(truncated, 0x1234, 53);
    CIO_CHECK(tc.has_value());
    CIO_CHECK(tc->truncated);
}

// ------------------------------------------------------- live resolver ---

// A minimal authoritative-ish stub that answers every A query with one address,
// so the resolver can be tested end to end without touching the network.
cio::Task<> stub_server(net::UdpSocket socket, cio::CancelToken quit) {
    std::vector<std::byte> buffer(4096);
    socket.set_cancel(quit);
    for (;;) {
        net::SocketAddr from;
        auto n = co_await socket.recv_from(buffer, from);
        if (!n) co_return;

        const std::span<const std::byte> query{buffer.data(), *n};
        const std::uint16_t id = be16(query, 0);
        const std::uint16_t qtype = be16(query, query.size() - 4);

        std::vector<std::byte> reply;
        if (qtype == static_cast<std::uint16_t>(dns::RecordType::a)) {
            reply = make_response(id, {"203.0.113.7"});
        } else {
            // No AAAA: answer with zero records so the dual lookup still
            // succeeds from the A side alone.
            reply = make_response(id, {});
            reply[6] = std::byte{0};
            reply[7] = std::byte{0};
        }
        if (auto sent = co_await socket.send_to(reply, from); !sent) co_return;
    }
}

void test_lookup_against_a_stub_server() {
    auto body = []() -> cio::Task<bool> {
        auto server = net::UdpSocket::bind(net::SocketAddr::loopback_v4(0));
        CIO_CHECK(server.has_value());
        const auto server_addr = server->local_addr().value();

        cio::CancelSource quit;
        auto serving = cio::spawn(stub_server(std::move(*server), quit.token()));

        dns::Config config;
        config.servers = {server_addr};
        config.timeout = 500ms;
        dns::Resolver resolver{config};

        auto addresses = co_await resolver.lookup("example.com", 443);
        CIO_CHECK(addresses.has_value());
        CIO_CHECK(!addresses->empty());
        bool found = false;
        for (const auto& a : *addresses) {
            if (a.to_string() == "203.0.113.7:443") found = true;
        }
        CIO_CHECK(found);

        // A literal is returned without a query at all.
        auto literal = co_await resolver.lookup("127.0.0.1", 8080);
        CIO_CHECK(literal.has_value());
        CIO_CHECK_EQ(literal->size(), std::size_t{1});
        CIO_CHECK_EQ(literal->front().to_string(), std::string("127.0.0.1:8080"));

        quit.cancel();
        co_await serving;
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

// A server that never answers: the attempt deadline must end the lookup, and
// it must not consume a blocking-pool thread while it waits.
void test_lookup_times_out_without_a_server() {
    auto body = []() -> cio::Task<bool> {
        // Bound but never read from, so queries are silently dropped.
        auto sink = net::UdpSocket::bind(net::SocketAddr::loopback_v4(0));
        CIO_CHECK(sink.has_value());

        dns::Config config;
        config.servers = {sink->local_addr().value()};
        config.timeout = 60ms;
        config.attempts = 1;
        dns::Resolver resolver{config};

        const auto started = cio::Clock::now();
        auto result = co_await resolver.lookup("example.com", 53);
        const auto elapsed = cio::Clock::now() - started;

        CIO_CHECK(!result.has_value());
        CIO_CHECK(elapsed < 5s);
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

// Cancellation must interrupt a lookup that is parked on the wire — the point
// of implementing DNS rather than calling getaddrinfo.
void test_lookup_is_cancellable_mid_flight() {
    auto body = []() -> cio::Task<bool> {
        auto sink = net::UdpSocket::bind(net::SocketAddr::loopback_v4(0));
        CIO_CHECK(sink.has_value());

        dns::Config config;
        config.servers = {sink->local_addr().value()};
        config.timeout = 10s;  // far longer than the cancellation
        config.attempts = 1;
        dns::Resolver resolver{config};

        cio::CancelSource stop;
        auto canceller = cio::spawn([](cio::CancelSource source) -> cio::Task<> {
            co_await cio::sleep(40ms);
            source.cancel();
        }(stop));

        const auto started = cio::Clock::now();
        auto result = co_await resolver.lookup("example.com", 53, stop.token());
        const auto elapsed = cio::Clock::now() - started;
        co_await canceller;

        CIO_CHECK(!result.has_value());
        CIO_CHECK(result.error().is(cio::Errc::cancelled));
        // Ended on the cancellation, not on the 10s attempt deadline.
        CIO_CHECK(elapsed < 5s);
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

void test_system_servers_parsing() {
    // Environment-dependent: only the shape is guaranteed.
    const auto servers = dns::system_servers();
    for (const auto& s : servers) {
        CIO_CHECK(s.valid());
        CIO_CHECK_EQ(s.port(), std::uint16_t{53});
    }
}

}  // namespace

int main() {
    RUN_TEST(test_query_encoding);
    RUN_TEST(test_query_encoding_rejects_bad_names);
    RUN_TEST(test_response_parsing);
    RUN_TEST(test_response_parsing_rejects_bad_messages);
    RUN_TEST(test_lookup_against_a_stub_server);
    RUN_TEST(test_lookup_times_out_without_a_server);
    RUN_TEST(test_lookup_is_cancellable_mid_flight);
    RUN_TEST(test_system_servers_parsing);
    return cio_test::summary();
}
