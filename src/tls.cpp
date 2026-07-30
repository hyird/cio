#include "cio/tls.hpp"

#include <openssl/err.h>
#include <openssl/ssl.h>
#include <openssl/x509v3.h>

#include <algorithm>
#include <cerrno>
#include <mutex>
#include <vector>
#include <utility>

namespace cio::tls {
namespace {

// OpenSSL 1.1+ initializes itself, but the algorithm tables are still worth
// forcing once so the first handshake does not pay for them under a lock.
void ensure_initialized() {
    static std::once_flag once;
    std::call_once(once, [] {
        ::OPENSSL_init_ssl(OPENSSL_INIT_LOAD_SSL_STRINGS |
                               OPENSSL_INIT_LOAD_CRYPTO_STRINGS,
                           nullptr);
    });
}

// OpenSSL failures do not map onto errno. Drain the thread's error queue so a
// later operation cannot inherit a stale entry, and report one cio error.
Error take_openssl_error() {
    unsigned long code = 0;
    unsigned long last = 0;
    while ((code = ::ERR_get_error()) != 0) last = code;
    (void)last;
    return Error{Errc::broken};
}

void discard_openssl_errors() {
    while (::ERR_get_error() != 0) {
    }
}

}  // namespace

// The BIO pair keeps OpenSSL entirely in memory: it never touches the
// descriptor itself. Ciphertext moves between the network BIO and the socket
// here, which is what lets every wait go through the runtime's reactor instead
// of a blocking socket callback.
struct TlsStream::State {
    net::TcpConn socket;
    SSL_CTX* context = nullptr;
    SSL* ssl = nullptr;
    BIO* network = nullptr;  // owned by us; the internal BIO is owned by SSL
    Config config;
    bool is_client = true;
    bool configured = false;
    bool handshake_done = false;
    bool shutdown_sent = false;
    std::vector<std::byte> transfer;

    State() : transfer(16 * 1024) {}

    ~State() {
        if (ssl != nullptr) ::SSL_free(ssl);
        if (network != nullptr) ::BIO_free(network);
        if (context != nullptr) ::SSL_CTX_free(context);
    }
};

namespace {

Result<void> configure(TlsStream::State& state) {
    if (state.configured) return ok();
    ensure_initialized();

    state.context = ::SSL_CTX_new(state.is_client ? ::TLS_client_method()
                                                  : ::TLS_server_method());
    if (state.context == nullptr) return take_openssl_error();

    // TLS 1.2 is the floor: everything below it is broken in ways that are not
    // this library's to work around.
    if (::SSL_CTX_set_min_proto_version(state.context, TLS1_2_VERSION) != 1) {
        return take_openssl_error();
    }

    if (state.is_client) {
        const Config& config = state.config;
        if (config.verify_peer) {
            ::SSL_CTX_set_verify(state.context, SSL_VERIFY_PEER, nullptr);
            if (config.ca_file.empty()) {
                if (::SSL_CTX_set_default_verify_paths(state.context) != 1) {
                    return take_openssl_error();
                }
            } else if (::SSL_CTX_load_verify_locations(
                           state.context, config.ca_file.c_str(), nullptr) != 1) {
                return take_openssl_error();
            }
        } else {
            ::SSL_CTX_set_verify(state.context, SSL_VERIFY_NONE, nullptr);
        }
    } else {
        const Config& config = state.config;
        if (config.certificate_file.empty() || config.private_key_file.empty()) {
            return Error{EINVAL};
        }
        if (::SSL_CTX_use_certificate_chain_file(
                state.context, config.certificate_file.c_str()) != 1) {
            return take_openssl_error();
        }
        if (::SSL_CTX_use_PrivateKey_file(state.context,
                                          config.private_key_file.c_str(),
                                          SSL_FILETYPE_PEM) != 1) {
            return take_openssl_error();
        }
        if (::SSL_CTX_check_private_key(state.context) != 1) {
            return take_openssl_error();
        }
    }

    state.ssl = ::SSL_new(state.context);
    if (state.ssl == nullptr) return take_openssl_error();

    BIO* internal = nullptr;
    if (::BIO_new_bio_pair(&internal, 0, &state.network, 0) != 1) {
        return take_openssl_error();
    }
    ::SSL_set_bio(state.ssl, internal, internal);

    if (state.is_client) {
        const std::string& name = state.config.server_name;
        if (!name.empty()) {
            if (::SSL_set_tlsext_host_name(state.ssl, name.c_str()) != 1) {
                return take_openssl_error();
            }
            if (state.config.verify_peer &&
                ::SSL_set1_host(state.ssl, name.c_str()) != 1) {
                return take_openssl_error();
            }
        }
        ::SSL_set_connect_state(state.ssl);
    } else {
        ::SSL_set_accept_state(state.ssl);
    }

    state.configured = true;
    return ok();
}

// Pushes any ciphertext OpenSSL has produced out to the socket.
Task<Result<void>> flush_network(TlsStream::State& state) {
    for (;;) {
        // BIO_pending is a macro, so it cannot be qualified.
        const int pending = BIO_pending(state.network);
        if (pending <= 0) co_return ok();

        const std::size_t want =
            std::min(static_cast<std::size_t>(pending), state.transfer.size());
        const int read = ::BIO_read(state.network, state.transfer.data(),
                                    static_cast<int>(want));
        if (read <= 0) co_return take_openssl_error();

        auto written = co_await cio::write_all(
            state.socket,
            std::span<const std::byte>{state.transfer.data(),
                                       static_cast<std::size_t>(read)});
        if (!written) co_return written.error();
    }
}

// Pulls one batch of ciphertext from the socket into OpenSSL. Reports
// Errc::closed at EOF so a truncated stream is not mistaken for success.
Task<Result<void>> fill_network(TlsStream::State& state) {
    auto n = co_await state.socket.read(state.transfer);
    if (!n) co_return n.error();
    if (*n == 0) co_return Error{Errc::closed};

    std::size_t offset = 0;
    while (offset < *n) {
        const int written =
            ::BIO_write(state.network, state.transfer.data() + offset,
                        static_cast<int>(*n - offset));
        if (written <= 0) co_return take_openssl_error();
        offset += static_cast<std::size_t>(written);
    }
    co_return ok();
}

// Runs one OpenSSL operation to completion, servicing its I/O demands.
// `attempt` returns the raw OpenSSL return code.
template <typename Attempt>
Task<Result<int>> pump(TlsStream::State& state, Attempt attempt) {
    for (;;) {
        discard_openssl_errors();
        const int rc = attempt();
        if (rc > 0) {
            // Even a successful call may have queued ciphertext to send.
            if (auto flushed = co_await flush_network(state); !flushed) {
                co_return flushed.error();
            }
            co_return rc;
        }

        const int reason = ::SSL_get_error(state.ssl, rc);
        if (reason == SSL_ERROR_WANT_WRITE || reason == SSL_ERROR_WANT_READ) {
            // Always drain first: OpenSSL often needs its own output delivered
            // before the peer will send what it is waiting for.
            if (auto flushed = co_await flush_network(state); !flushed) {
                co_return flushed.error();
            }
            if (reason == SSL_ERROR_WANT_READ) {
                if (auto filled = co_await fill_network(state); !filled) {
                    co_return filled.error();
                }
            }
            continue;
        }
        if (reason == SSL_ERROR_ZERO_RETURN) co_return 0;
        if (reason == SSL_ERROR_SYSCALL && ::ERR_peek_error() == 0) {
            co_return Error{Errc::closed};
        }
        co_return take_openssl_error();
    }
}

}  // namespace

TlsStream::~TlsStream() = default;
TlsStream::TlsStream(TlsStream&&) noexcept = default;
TlsStream& TlsStream::operator=(TlsStream&&) noexcept = default;

TlsStream client(net::TcpConn stream, Config config) {
    TlsStream tls;
    tls.state_ = std::make_unique<TlsStream::State>();
    tls.state_->socket = std::move(stream);
    tls.state_->config = std::move(config);
    tls.state_->is_client = true;
    return tls;
}

TlsStream server(net::TcpConn stream, Config config) {
    TlsStream tls;
    tls.state_ = std::make_unique<TlsStream::State>();
    tls.state_->socket = std::move(stream);
    tls.state_->config = std::move(config);
    tls.state_->is_client = false;
    return tls;
}

Task<Result<void>> TlsStream::handshake() {
    if (state_ == nullptr) co_return Error{EBADF};
    if (state_->handshake_done) co_return ok();
    if (auto configured = configure(*state_); !configured) {
        co_return configured.error();
    }

    State& state = *state_;
    auto rc = co_await pump(state, [&state] { return ::SSL_do_handshake(state.ssl); });
    if (!rc) co_return rc.error();
    if (*rc <= 0) co_return Error{Errc::closed};

    state.handshake_done = true;
    co_return ok();
}

Task<Result<std::size_t>> TlsStream::read(std::span<std::byte> buffer) {
    if (state_ == nullptr) co_return Error{EBADF};
    if (buffer.empty()) co_return std::size_t{0};
    if (!state_->handshake_done) {
        if (auto shaken = co_await handshake(); !shaken) co_return shaken.error();
    }

    State& state = *state_;
    auto rc = co_await pump(state, [&state, buffer] {
        return ::SSL_read(state.ssl, buffer.data(),
                          static_cast<int>(buffer.size()));
    });
    if (!rc) {
        // A clean close_notify is end of stream, not an error.
        if (rc.error().is(Errc::closed)) co_return std::size_t{0};
        co_return rc.error();
    }
    if (*rc <= 0) co_return std::size_t{0};
    co_return static_cast<std::size_t>(*rc);
}

Task<Result<std::size_t>> TlsStream::write(std::span<const std::byte> buffer) {
    if (state_ == nullptr) co_return Error{EBADF};
    if (buffer.empty()) co_return std::size_t{0};
    if (!state_->handshake_done) {
        if (auto shaken = co_await handshake(); !shaken) co_return shaken.error();
    }

    State& state = *state_;
    auto rc = co_await pump(state, [&state, buffer] {
        return ::SSL_write(state.ssl, buffer.data(),
                           static_cast<int>(buffer.size()));
    });
    if (!rc) co_return rc.error();
    if (*rc <= 0) co_return Error{Errc::closed};
    co_return static_cast<std::size_t>(*rc);
}

Task<Result<void>> TlsStream::shutdown() {
    if (state_ == nullptr) co_return ok();
    State& state = *state_;
    if (state.shutdown_sent || !state.handshake_done) {
        state.socket.close();
        co_return ok();
    }
    state.shutdown_sent = true;

    // One close_notify is enough; waiting for the peer's is optional and would
    // hang against a peer that simply closes.
    discard_openssl_errors();
    (void)::SSL_shutdown(state.ssl);
    auto flushed = co_await flush_network(state);
    state.socket.close();
    if (!flushed) co_return flushed.error();
    co_return ok();
}

Result<net::SocketAddr> TlsStream::local_addr() const {
    if (state_ == nullptr) return Error{EBADF};
    return state_->socket.local_addr();
}

Result<net::SocketAddr> TlsStream::remote_addr() const {
    if (state_ == nullptr) return Error{EBADF};
    return state_->socket.remote_addr();
}

void TlsStream::set_deadline(TimePoint deadline) {
    if (state_ != nullptr) state_->socket.set_deadline(deadline);
}

void TlsStream::set_read_deadline(TimePoint deadline) {
    if (state_ != nullptr) state_->socket.set_read_deadline(deadline);
}

void TlsStream::set_write_deadline(TimePoint deadline) {
    if (state_ != nullptr) state_->socket.set_write_deadline(deadline);
}

void TlsStream::clear_deadline() {
    if (state_ != nullptr) state_->socket.clear_deadline();
}

void TlsStream::clear_read_deadline() {
    if (state_ != nullptr) state_->socket.clear_read_deadline();
}

void TlsStream::clear_write_deadline() {
    if (state_ != nullptr) state_->socket.clear_write_deadline();
}

void TlsStream::set_cancel(CancelToken token) {
    if (state_ != nullptr) state_->socket.set_cancel(std::move(token));
}

void TlsStream::clear_cancel() {
    if (state_ != nullptr) state_->socket.clear_cancel();
}

void TlsStream::close() {
    if (state_ != nullptr) state_->socket.close();
}

std::string TlsStream::protocol_version() const {
    if (state_ == nullptr || state_->ssl == nullptr) return {};
    const char* name = ::SSL_get_version(state_->ssl);
    return name != nullptr ? std::string(name) : std::string{};
}

}  // namespace cio::tls
