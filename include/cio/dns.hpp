// A DNS resolver that speaks the protocol.
//
//     cio::dns::Resolver resolver;
//     auto addrs = co_await resolver.lookup("example.com", 443);
//
// This is an alternative to net::Resolver, not a replacement. The two make
// opposite trades and both are kept:
//
//   net::Resolver  getaddrinfo() on the blocking pool. Honours /etc/hosts,
//                  nsswitch.conf, mDNS and every other NSS module, which is
//                  what most programs actually want. Costs a pool thread per
//                  concurrent lookup and cannot be interrupted once started.
//   dns::Resolver  DNS/UDP over the runtime's own sockets. Fully asynchronous,
//                  cancellable mid-flight, and costs no pool thread. Sees only
//                  DNS: no /etc/hosts, no NSS, no mDNS.
//
// A program that resolves public names at high concurrency wants this one. A
// program that must agree with `getent hosts` wants the other.
//
// NOT IMPLEMENTED, deliberately: no cache (a cache needs an explicit TTL,
// negative-caching and invalidation policy, and belongs in a wrapper), no
// DNSSEC validation, no recursive resolution — this is a stub resolver that
// talks to the configured recursive servers.
#pragma once

#include <chrono>
#include <cstdint>
#include <string>
#include <vector>

#include "cio/clock.hpp"
#include "cio/group.hpp"
#include "cio/net.hpp"
#include "cio/result.hpp"
#include "cio/task.hpp"

namespace cio::dns {

enum class RecordType : std::uint16_t {
    a = 1,
    aaaa = 28,
};

struct Config {
    // Servers to query, in order. Empty means read /etc/resolv.conf.
    std::vector<net::SocketAddr> servers;
    // Per-attempt timeout. Zero selects 2 seconds, matching resolv.conf's
    // default.
    Duration timeout{};
    // Attempts per server before moving to the next.
    unsigned attempts = 2;
    // Which families to query. Both are queried concurrently when set.
    bool ipv4 = true;
    bool ipv6 = true;
};

// Parses the nameserver lines of /etc/resolv.conf. Returns an empty vector
// rather than an error when the file is missing or lists none.
std::vector<net::SocketAddr> system_servers(std::uint16_t port = 53);

class Resolver {
public:
    Resolver() = default;
    explicit Resolver(Config config) : config_(std::move(config)) {}

    // Resolves `host` to addresses carrying `port`.
    //
    // A literal address is returned as-is without a query. A name with no
    // matching record is Errc::closed-free: it returns ENOENT, the same as the
    // system resolver's empty result.
    Task<Result<std::vector<net::SocketAddr>>> lookup(
        std::string host, std::uint16_t port, CancelToken cancel = {}) const;

    // One record type, for callers that know which family they want.
    Task<Result<std::vector<net::SocketAddr>>> lookup_type(
        std::string host, std::uint16_t port, RecordType type,
        CancelToken cancel = {}) const;

    const Config& config() const noexcept { return config_; }

private:
    Config config_{};
};

namespace detail {

// Exposed for testing: the wire codec is where a DNS implementation gets its
// bugs, so it is reachable without a network.
Result<std::vector<std::byte>> build_query(std::string_view name,
                                           RecordType type,
                                           std::uint16_t id);

struct Answer {
    std::vector<net::SocketAddr> addresses;
    bool truncated = false;
};

Result<Answer> parse_response(std::span<const std::byte> message,
                              std::uint16_t expected_id, std::uint16_t port);

}  // namespace detail
}  // namespace cio::dns
