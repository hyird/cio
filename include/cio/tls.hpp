// TLS over an existing stream.
//
//     auto tcp = co_await cio::net::TcpStream::connect("example.com", 443);
//     auto stream = cio::tls::client(std::move(*tcp), {.server_name = "example.com"});
//     co_await stream.handshake();
//     co_await cio::write_all(stream, request);
//
// This is an optional target: enabling it links OpenSSL, which is why it is not
// part of the core library. Build with -DCIO_TLS=ON and link cio::tls.
//
// TlsStream satisfies the AsyncReader/AsyncWriter concepts, so read_exact(),
// write_all() and copy() work on it unchanged. OpenSSL's WANT_READ/WANT_WRITE
// are translated into waits on the underlying TcpStream; the record layer means
// one application-level read may drive several socket reads and vice versa.
//
// OWNERSHIP: move-only, and the same one-reader/one-writer rule as the
// underlying socket. Deadlines and close() are delegated to that socket, so
// they interrupt a parked handshake or transfer exactly as they would a plain
// TCP operation.
#pragma once

#include <cstddef>
#include <memory>
#include <span>
#include <string>

#include "cio/io.hpp"
#include "cio/net.hpp"
#include "cio/result.hpp"
#include "cio/task.hpp"

namespace cio::tls {

struct ClientConfig {
    // SNI and certificate hostname to verify against. Empty disables both,
    // which is only appropriate when the peer is identified some other way.
    std::string server_name;
    // Verification is on by default; turning it off makes the connection
    // encrypted but not authenticated.
    bool verify_peer = true;
    // Optional PEM bundle. Empty uses the system default trust store.
    std::string ca_file;
};

struct ServerConfig {
    std::string certificate_file;  // PEM
    std::string private_key_file;  // PEM
};

class TlsStream {
public:
    TlsStream() = default;
    ~TlsStream();

    TlsStream(TlsStream&&) noexcept;
    TlsStream& operator=(TlsStream&&) noexcept;
    TlsStream(const TlsStream&) = delete;
    TlsStream& operator=(const TlsStream&) = delete;

    bool valid() const noexcept { return state_ != nullptr; }

    // Drives the handshake to completion. Must finish before read()/write().
    Task<Result<void>> handshake();

    Task<Result<std::size_t>> read(std::span<std::byte> buffer);
    Task<Result<std::size_t>> write(std::span<const std::byte> buffer);

    // Sends close_notify, then closes the socket. Idempotent.
    Task<Result<void>> shutdown();

    void set_deadline(TimePoint deadline);
    void clear_deadline();
    void close();

    // The negotiated protocol version name, or empty before the handshake.
    std::string protocol_version() const;

    // Opaque to every consumer: only tls.cpp ever sees the definition.
    struct State;

private:
    friend TlsStream client(net::TcpStream stream, ClientConfig config);
    friend TlsStream server(net::TcpStream stream, ServerConfig config);

    std::unique_ptr<State> state_;
};

// Wrap an already-connected stream. Errors in the configuration surface from
// handshake(), not here, so both factories stay non-throwing and non-suspending.
TlsStream client(net::TcpStream stream, ClientConfig config = {});
TlsStream server(net::TcpStream stream, ServerConfig config);

}  // namespace cio::tls
