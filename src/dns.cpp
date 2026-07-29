#include "cio/dns.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include <atomic>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <strings.h>
#include <fstream>
#include <span>
#include <string_view>

#include "cio/select.hpp"
#include "cio/spawn.hpp"

namespace cio::dns {
namespace {

constexpr std::size_t kHeaderBytes = 12;
constexpr std::size_t kMaxUdpMessage = 4096;
constexpr auto kDefaultTimeout = std::chrono::seconds(2);
// A compression pointer chain longer than this is a malformed or hostile
// message; the bound is what makes name decoding terminate.
constexpr unsigned kMaxPointerHops = 64;

std::uint16_t read_u16(std::span<const std::byte> m, std::size_t at) noexcept {
    return static_cast<std::uint16_t>(
        (static_cast<unsigned>(std::to_integer<unsigned char>(m[at])) << 8) |
        static_cast<unsigned>(std::to_integer<unsigned char>(m[at + 1])));
}

void push_u16(std::vector<std::byte>& out, std::uint16_t value) {
    out.push_back(static_cast<std::byte>((value >> 8) & 0xFF));
    out.push_back(static_cast<std::byte>(value & 0xFF));
}

// Skips a (possibly compressed) name, returning the offset just past it.
// Compression pointers are followed only to count the name's encoded length,
// which is why the caller gets back a position in the *original* stream.
Result<std::size_t> skip_name(std::span<const std::byte> m, std::size_t at) {
    unsigned hops = 0;
    for (;;) {
        if (at >= m.size()) return Error{EBADMSG};
        const auto length =
            static_cast<unsigned>(std::to_integer<unsigned char>(m[at]));
        if (length == 0) return at + 1;
        if ((length & 0xC0) == 0xC0) {
            if (at + 1 >= m.size()) return Error{EBADMSG};
            // A pointer is always the last thing in a name.
            return at + 2;
        }
        if ((length & 0xC0) != 0) return Error{EBADMSG};
        at += 1 + length;
        if (++hops > kMaxPointerHops) return Error{EBADMSG};
    }
}

// Encodes a host name as a sequence of length-prefixed labels.
Result<void> encode_name(std::string_view name, std::vector<std::byte>& out) {
    if (name.size() > 253) return Error{EINVAL};
    // A single trailing dot is the root and carries no label.
    if (!name.empty() && name.back() == '.') name.remove_suffix(1);
    if (name.empty()) return Error{EINVAL};

    std::size_t start = 0;
    while (start <= name.size()) {
        const std::size_t dot = name.find('.', start);
        const std::size_t end = dot == std::string_view::npos ? name.size() : dot;
        const std::size_t length = end - start;
        if (length == 0 || length > 63) return Error{EINVAL};
        out.push_back(static_cast<std::byte>(length));
        for (std::size_t i = start; i < end; ++i) {
            out.push_back(static_cast<std::byte>(name[i]));
        }
        if (dot == std::string_view::npos) break;
        start = dot + 1;
    }
    out.push_back(std::byte{0});
    return ok();
}

net::SocketAddr addr_from_v4(const std::byte* raw, std::uint16_t port) {
    sockaddr_in sa{};
    sa.sin_family = AF_INET;
    sa.sin_port = ::htons(port);
    std::memcpy(&sa.sin_addr, raw, 4);
    return net::SocketAddr::from_raw(&sa, sizeof(sa));
}

net::SocketAddr addr_from_v6(const std::byte* raw, std::uint16_t port) {
    sockaddr_in6 sa{};
    sa.sin6_family = AF_INET6;
    sa.sin6_port = ::htons(port);
    std::memcpy(&sa.sin6_addr, raw, 16);
    return net::SocketAddr::from_raw(&sa, sizeof(sa));
}

// Query IDs only need to be unpredictable enough that an off-path attacker
// cannot trivially guess the next one; the socket is also connected to one
// server and the response is matched on id, name-count and question.
std::uint16_t next_query_id() noexcept {
    static std::atomic<std::uint32_t> counter{
        static_cast<std::uint32_t>(now_ns())};
    const std::uint32_t value =
        counter.fetch_add(0x9E3779B9u, std::memory_order_relaxed);
    // Mix so successive ids are not a visible arithmetic sequence.
    std::uint32_t x = value;
    x ^= x >> 16;
    x *= 0x7FEB352Du;
    x ^= x >> 15;
    return static_cast<std::uint16_t>(x & 0xFFFF);
}

}  // namespace

namespace detail {

Result<std::vector<std::byte>> build_query(std::string_view name,
                                           RecordType type,
                                           std::uint16_t id) {
    std::vector<std::byte> out;
    out.reserve(kHeaderBytes + name.size() + 6);

    push_u16(out, id);
    push_u16(out, 0x0100);  // standard query, recursion desired
    push_u16(out, 1);       // one question
    push_u16(out, 0);       // no answers
    push_u16(out, 0);       // no authority
    push_u16(out, 0);       // no additional

    if (auto encoded = encode_name(name, out); !encoded) return encoded.error();
    push_u16(out, static_cast<std::uint16_t>(type));
    push_u16(out, 1);  // IN
    return out;
}

Result<Answer> parse_response(std::span<const std::byte> message,
                              std::uint16_t expected_id, std::uint16_t port) {
    if (message.size() < kHeaderBytes) return Error{EBADMSG};
    if (read_u16(message, 0) != expected_id) return Error{EBADMSG};

    const std::uint16_t flags = read_u16(message, 2);
    Answer answer;
    answer.truncated = (flags & 0x0200) != 0;

    const unsigned rcode = flags & 0x000F;
    if (rcode == 3) return Error{ENOENT};        // NXDOMAIN
    if (rcode != 0 && !answer.truncated) return Error{EBADMSG};

    const std::uint16_t questions = read_u16(message, 4);
    const std::uint16_t answers = read_u16(message, 6);

    std::size_t at = kHeaderBytes;
    for (std::uint16_t i = 0; i < questions; ++i) {
        auto next = skip_name(message, at);
        if (!next) return next.error();
        at = *next + 4;  // qtype + qclass
        if (at > message.size()) return Error{EBADMSG};
    }

    for (std::uint16_t i = 0; i < answers; ++i) {
        auto next = skip_name(message, at);
        if (!next) return next.error();
        at = *next;
        if (at + 10 > message.size()) return Error{EBADMSG};

        const std::uint16_t type = read_u16(message, at);
        const std::uint16_t klass = read_u16(message, at + 2);
        const std::uint16_t length = read_u16(message, at + 8);
        at += 10;
        if (at + length > message.size()) return Error{EBADMSG};

        // CNAMEs are followed by the recursive server, so the answer section
        // already carries the final A/AAAA records; unknown types are skipped
        // rather than treated as an error.
        if (klass == 1 && type == static_cast<std::uint16_t>(RecordType::a) &&
            length == 4) {
            answer.addresses.push_back(addr_from_v4(message.data() + at, port));
        } else if (klass == 1 &&
                   type == static_cast<std::uint16_t>(RecordType::aaaa) &&
                   length == 16) {
            answer.addresses.push_back(addr_from_v6(message.data() + at, port));
        }
        at += length;
    }
    return answer;
}

}  // namespace detail

namespace {

// Go's built-in resolver reads /etc/hosts before querying, and a resolver that
// skipped it would answer differently from every other program on the machine
// for exactly the names an operator is most likely to have overridden.
//
// Only the hosts file is honoured here; nsswitch.conf ordering, NIS and mDNS
// are not, which is the documented boundary of this resolver.
std::vector<net::SocketAddr> hosts_file_lookup(std::string_view name,
                                               std::uint16_t port) {
    std::vector<net::SocketAddr> found;
    std::ifstream file("/etc/hosts");
    if (!file) return found;

    std::string line;
    while (std::getline(file, line)) {
        const std::size_t hash = line.find('#');
        if (hash != std::string::npos) line.resize(hash);

        std::size_t at = line.find_first_not_of(" \t");
        if (at == std::string::npos) continue;
        const std::size_t address_end = line.find_first_of(" \t", at);
        if (address_end == std::string::npos) continue;
        const std::string address = line.substr(at, address_end - at);

        // Every remaining field is a name for that address.
        bool matched = false;
        std::size_t field = address_end;
        for (;;) {
            field = line.find_first_not_of(" \t", field);
            if (field == std::string::npos) break;
            const std::size_t end = line.find_first_of(" \t", field);
            const std::size_t length =
                end == std::string::npos ? line.size() - field : end - field;
            // Host names are case-insensitive.
            if (length == name.size() &&
                ::strncasecmp(line.data() + field, name.data(), length) == 0) {
                matched = true;
                break;
            }
            if (end == std::string::npos) break;
            field = end;
        }
        if (!matched) continue;

        if (auto parsed = net::SocketAddr::parse(address, port); parsed) {
            found.push_back(*parsed);
        }
    }
    return found;
}

}  // namespace

std::vector<net::SocketAddr> system_servers(std::uint16_t port) {
    std::vector<net::SocketAddr> servers;
    std::ifstream file("/etc/resolv.conf");
    if (!file) return servers;

    std::string line;
    while (std::getline(file, line)) {
        const std::size_t hash = line.find('#');
        if (hash != std::string::npos) line.resize(hash);
        const std::size_t semi = line.find(';');
        if (semi != std::string::npos) line.resize(semi);

        std::size_t at = line.find_first_not_of(" \t");
        if (at == std::string::npos) continue;
        if (line.compare(at, 10, "nameserver") != 0) continue;
        at = line.find_first_not_of(" \t", at + 10);
        if (at == std::string::npos) continue;
        const std::size_t end = line.find_first_of(" \t%", at);
        const std::string host =
            line.substr(at, end == std::string::npos ? std::string::npos
                                                     : end - at);

        if (auto parsed = net::SocketAddr::parse(host, port); parsed) {
            servers.push_back(*parsed);
        }
    }
    return servers;
}

namespace {

// One query against one server, with its own socket so a late reply from a
// previous attempt cannot be mistaken for this one's.
Task<Result<detail::Answer>> query_once(net::SocketAddr server,
                                        std::string name, RecordType type,
                                        std::uint16_t port, Duration timeout,
                                        CancelToken cancel) {
    const std::uint16_t id = next_query_id();
    auto request = detail::build_query(name, type, id);
    if (!request) co_return request.error();

    auto socket = net::UdpSocket::bind(
        server.family() == AF_INET6 ? net::SocketAddr::any_v6(0)
                                    : net::SocketAddr::any_v4(0));
    if (!socket) co_return socket.error();

    // Cancellation and the attempt deadline both ride the socket, so neither
    // needs a watcher task and both interrupt a parked recvfrom.
    if (cancel) socket->set_cancel(cancel);
    socket->set_timeout(timeout);

    if (auto sent = co_await socket->send_to(*request, server); !sent) {
        co_return sent.error();
    }

    // Loop rather than accept the first datagram: an off-path reply with the
    // wrong id must not end the attempt.
    std::vector<std::byte> buffer(kMaxUdpMessage);
    for (;;) {
        net::SocketAddr from;
        auto received = co_await socket->recv_from(buffer, from);
        if (!received) co_return received.error();

        auto answer = detail::parse_response(
            std::span<const std::byte>{buffer.data(), *received}, id, port);
        if (answer) co_return answer;
        // A mismatched id is not a failure of this attempt; a malformed or
        // negative answer is.
        if (!answer.error().is(EBADMSG)) co_return answer.error();
    }
}

Task<Result<std::vector<net::SocketAddr>>> resolve_type(
    const Config& config, std::string name, std::uint16_t port,
    RecordType type, CancelToken cancel) {
    std::vector<net::SocketAddr> servers = config.servers;
    if (servers.empty()) servers = system_servers();
    if (servers.empty()) co_return Error{ECONNREFUSED};

    const Duration timeout =
        config.timeout > Duration::zero() ? config.timeout
                                          : Duration{kDefaultTimeout};
    const unsigned attempts = config.attempts == 0 ? 1 : config.attempts;

    Error last{ETIMEDOUT};
    for (unsigned attempt = 0; attempt < attempts; ++attempt) {
        for (const auto& server : servers) {
            if (cancel && cancel.cancelled()) co_return Error{Errc::cancelled};

            auto answer = co_await query_once(server, name, type, port,
                                              timeout, cancel);
            if (answer) {
                // Truncation means the answer did not fit in a datagram. TCP
                // fallback is not implemented; report it rather than silently
                // returning a partial address list.
                if (answer->truncated && answer->addresses.empty()) {
                    last = Error{EMSGSIZE};
                    continue;
                }
                co_return std::move(answer->addresses);
            }
            last = answer.error();
            // A definitive answer is not worth retrying against other servers.
            if (last.is(ENOENT) || last.is(Errc::cancelled)) co_return last;
        }
    }
    co_return last;
}

}  // namespace

Task<Result<std::vector<net::SocketAddr>>> Resolver::lookup_type(
    std::string host, std::uint16_t port, RecordType type,
    CancelToken cancel) const {
    if (auto literal = net::SocketAddr::parse(host, port); literal) {
        co_return std::vector<net::SocketAddr>{*literal};
    }
    if (config_.use_hosts_file) {
        const int want =
            type == RecordType::aaaa ? AF_INET6 : AF_INET;
        std::vector<net::SocketAddr> from_hosts;
        for (auto& entry : hosts_file_lookup(host, port)) {
            if (entry.family() == want) from_hosts.push_back(entry);
        }
        if (!from_hosts.empty()) co_return from_hosts;
    }
    co_return co_await resolve_type(config_, std::move(host), port, type,
                                    std::move(cancel));
}

Task<Result<std::vector<net::SocketAddr>>> Resolver::lookup(
    std::string host, std::uint16_t port, CancelToken cancel) const {
    if (auto literal = net::SocketAddr::parse(host, port); literal) {
        co_return std::vector<net::SocketAddr>{*literal};
    }
    // The hosts file wins over the network, as it does for every other
    // resolver on the machine.
    if (config_.use_hosts_file) {
        auto from_hosts = hosts_file_lookup(host, port);
        if (!from_hosts.empty()) co_return from_hosts;
    }
    if (!config_.ipv4 && !config_.ipv6) co_return Error{EINVAL};
    if (!config_.ipv6) {
        co_return co_await resolve_type(config_, std::move(host), port,
                                        RecordType::a, std::move(cancel));
    }
    if (!config_.ipv4) {
        co_return co_await resolve_type(config_, std::move(host), port,
                                        RecordType::aaaa, std::move(cancel));
    }

    // Both families are queried concurrently: serialising them would make a
    // dual-stack lookup cost two round trips.
    auto v6 = spawn(resolve_type(config_, host, port, RecordType::aaaa, cancel));
    auto v4 = co_await resolve_type(config_, host, port, RecordType::a, cancel);
    auto v6_result = co_await v6;

    std::vector<net::SocketAddr> merged;
    // IPv6 first, matching the system resolver's usual preference.
    if (v6_result) {
        merged.insert(merged.end(), v6_result->begin(), v6_result->end());
    }
    if (v4) merged.insert(merged.end(), v4->begin(), v4->end());

    if (merged.empty()) {
        if (!v4 && v4.error().is(Errc::cancelled)) co_return v4.error();
        if (!v6_result && v6_result.error().is(Errc::cancelled)) {
            co_return v6_result.error();
        }
        co_return Error{ENOENT};
    }
    co_return merged;
}

}  // namespace cio::dns
