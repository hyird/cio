// TLS over an existing stream.
//
//     auto tcp = co_await cio::net::TcpConn::dial("example.com", 443);
//     auto stream = cio::tls::client(std::move(*tcp), {.server_name = "example.com"});
//     co_await stream.handshake();
//     co_await stream.write(request);
//
// This is an optional target: enabling it links OpenSSL, which is why it is not
// part of the core library. Build with -DCIO_TLS=ON and link cio::tls.
//
// Conn satisfies the io::Reader/io::Writer concepts, so io::read_full(),
// write_all() and copy() work on it unchanged. OpenSSL's WANT_READ/WANT_WRITE
// are translated into waits on the underlying TcpConn; the record layer means
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
#include <vector>

#include "cio/io.hpp"
#include "cio/net.hpp"
#include "cio/result.hpp"
#include "cio/task.hpp"

namespace cio::tls {

// A certificate: a PEM chain and its private key, as Go's tls.Certificate is
// (paths here rather than DER bytes, because cio has no certificate-loading API
// and a file is the simplest handle). A server presents one; a client presents
// one only when a server asks for it, which is mutual TLS.
//
// The SNI names a server certificate serves are read from its own
// subjectAltNames, exactly as Go matches a ClientHello's ServerName against each
// certificate's Leaf — a certificate already carries the names it is valid for,
// so they are not repeated in configuration.
struct Certificate {
    std::string certificate_file;  // PEM chain
    std::string private_key_file;  // PEM key
};

// Go's tls.ClientAuthType: what a server asks of a client's certificate.
enum class ClientAuth {
    none,                // NoClientCert
    request,             // RequestClientCert: ask, accept whatever comes back
    require_any,         // RequireAnyClientCert: demand one, do not verify it
    verify_if_given,     // VerifyClientCertIfGiven
    require_and_verify,  // RequireAndVerifyClientCert: mutual TLS
};

// Go's ClientSessionCache, as a concrete LRU rather than an interface: one
// implementation does not need a vtable. Share one across the connections that
// should resume each other's sessions — it is internally synchronized, so
// several workers may use it at once.
//
// Resumption skips the certificate exchange on a reconnect, which is most of a
// handshake's cost. A client only resumes when it is given a cache; Go's default
// is likewise no cache.
class SessionCache {
public:
    explicit SessionCache(std::size_t capacity = 64);
    ~SessionCache();

    SessionCache(const SessionCache&) = delete;
    SessionCache& operator=(const SessionCache&) = delete;

    // Internal: the void* is an OpenSSL SSL_SESSION, kept opaque so this header
    // does not drag in OpenSSL. Not meant to be called by consumers.
    void* take(const std::string& key);
    void store(const std::string& key, void* session);

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

// Go's NewLRUClientSessionCache.
std::shared_ptr<SessionCache> new_lru_client_session_cache(
    std::size_t capacity = 64);

// One configuration for both roles, as Go's tls.Config is. Which fields matter
// depends on the role: a client reads server_name, verify_peer, ca_file and
// next_protos; a server reads the certificate list and next_protos.
struct Config {
    // SNI and certificate hostname to verify against. Empty disables both,
    // which is only appropriate when the peer is identified some other way.
    // Client only. Go's Config.ServerName.
    std::string server_name;
    // Verification is on by default; turning it off makes the connection
    // encrypted but not authenticated. Go spells this InsecureSkipVerify, in
    // the negative, so that the unsafe choice has to be written out.
    bool verify_peer = true;
    // Optional PEM trust bundle. Empty uses the system default store.
    // Client only.
    std::string ca_file;

    // A single certificate, the common case. Equivalent to one entry in
    // `certificates`, and ignored when `certificates` is non-empty. A server
    // presents it; a client presents it when asked, for mutual TLS.
    std::string certificate_file;
    std::string private_key_file;
    // Go's Config.Certificates. On a server, one or more certificates selected
    // by SNI against each certificate's own DNS names; the first is the default
    // when no name matches, as Go uses Certificates[0]. On a client, the first
    // entry is the certificate presented when a server requests one. When both
    // this and certificate_file are set, this wins.
    std::vector<Certificate> certificates;

    // Go's Config.ClientAuth: whether to ask a client for a certificate, and
    // whether to insist it verifies. Server only.
    ClientAuth client_auth = ClientAuth::none;
    // Go's Config.ClientCAs: PEM bundle that client certificates are verified
    // against, and whose names are advertised to the client so it knows which
    // certificate to offer. Server only; empty uses the system store.
    std::string client_ca_file;

    // Go's Config.SessionTicketsDisabled. Server only.
    bool session_tickets_disabled = false;
    // Go's Config.SetSessionTicketKeys: the secret that session tickets are
    // sealed under. At least 32 bytes; it is expanded with HKDF-SHA256 to
    // whatever length the OpenSSL in use wants, which is not the same across
    // versions, so one configuration keeps working when the library moves.
    //
    // Every connection builds its own OpenSSL context, so without this each
    // would invent its own ticket key and no client could ever resume against a
    // later connection. Setting the same secret on every connection of one
    // server is what makes server-side resumption work. Keep it secret and
    // rotate it: it protects every ticket issued under it. Server only.
    std::string session_ticket_key;

    // Go's Config.ClientSessionCache: enables resumption when set, and is where
    // resumable sessions are kept. Share one cache across the connections that
    // should resume each other's sessions. Client only.
    std::shared_ptr<SessionCache> session_cache;

    // Go's Config.NextProtos: ALPN protocol identifiers, in preference order —
    // e.g. {"h2", "http/1.1"}. A client offers them; a server picks the first
    // that a client also offers. HTTP/2 over TLS is negotiated here and nowhere
    // else: without "h2" in this list on both ends, a peer must fall back to
    // HTTP/1.1. Empty disables ALPN.
    std::vector<std::string> next_protos;
};

class Conn {
public:
    Conn() = default;
    ~Conn();

    Conn(Conn&&) noexcept;
    Conn& operator=(Conn&&) noexcept;
    Conn(const Conn&) = delete;
    Conn& operator=(const Conn&) = delete;

    bool valid() const noexcept { return state_ != nullptr; }

    // Drives the handshake to completion. Must finish before read()/write().
    Task<Result<void>> handshake();

    Task<Result<std::size_t>> read(std::span<std::byte> buffer);
    // Full write or error, per Go's io.Writer contract.
    Task<Result<std::size_t>> write(std::span<const std::byte> buffer);

    // Sends close_notify, then closes the socket. Idempotent.
    Task<Result<void>> shutdown();

    // The full net.Conn surface, delegated to the underlying socket, so a
    // Conn satisfies net::Conn exactly as Go's tls.Conn implements
    // net.Conn and a generic helper works over plaintext and TLS unchanged.
    Result<net::SocketAddr> local_addr() const;
    Result<net::SocketAddr> remote_addr() const;
    void set_deadline(TimePoint deadline);
    void set_read_deadline(TimePoint deadline);
    void set_write_deadline(TimePoint deadline);
    void clear_deadline();
    void clear_read_deadline();
    void clear_write_deadline();
    void set_cancel(CancelToken token);
    void clear_cancel();
    void close();

    // The negotiated protocol version name, or empty before the handshake.
    // Go's ConnectionState().Version, as a string.
    std::string protocol_version() const;

    // Go's ConnectionState().NegotiatedProtocol: the ALPN protocol both ends
    // agreed on ("h2", "http/1.1"), or empty when ALPN did not select one. A
    // server dispatches HTTP/2 vs HTTP/1.1 on this.
    std::string negotiated_protocol() const;

    // Go's ConnectionState().ServerName: the SNI host name the client sent,
    // empty on a client or when none was sent. This is the name the server
    // certificate was selected for.
    std::string server_name() const;

    // Go's ConnectionState().DidResume: whether this handshake resumed an
    // earlier session instead of doing the full certificate exchange.
    bool did_resume() const;

    // The subject name of the peer's certificate, empty when it presented none.
    // Go returns parsed x509 certificates here; cio returns the subject as a
    // string, because a certificate type is not something this library should
    // grow, and the subject is what an authorization check reads.
    std::string peer_certificate_subject() const;

    // Opaque to every consumer: only tls.cpp ever sees the definition.
    struct State;

private:
    friend Conn client(net::TcpConn stream, Config config);
    friend Conn server(net::TcpConn stream, Config config);

    std::unique_ptr<State> state_;
};

// Wrap an already-connected stream. Errors in the configuration surface from
// handshake(), not here, so both factories stay non-throwing and non-suspending.
Conn client(net::TcpConn stream, Config config = {});
Conn server(net::TcpConn stream, Config config);

static_assert(net::Conn<Conn>);

}  // namespace cio::tls
