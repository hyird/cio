#include "cio/tls.hpp"

#include <openssl/err.h>
#include <openssl/kdf.h>
#include <openssl/ssl.h>
#include <openssl/x509v3.h>

#include <algorithm>
#include <cerrno>
#include <list>
#include <mutex>
#include <unordered_map>
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

// A bounded LRU of SSL_SESSIONs, keyed by the endpoint they were established
// with. Guarded by a plain mutex rather than cio::Mutex: the critical section is
// a map lookup with no I/O in it, so suspending a task around it would cost more
// than it saves.
struct SessionCache::Impl {
    std::mutex mutex;
    std::size_t capacity;
    std::list<std::string> order;  // front = most recently used
    struct Entry {
        SSL_SESSION* session;
        std::list<std::string>::iterator position;
    };
    std::unordered_map<std::string, Entry> entries;

    explicit Impl(std::size_t cap) : capacity(cap == 0 ? 1 : cap) {}

    ~Impl() {
        for (auto& [key, entry] : entries) ::SSL_SESSION_free(entry.session);
    }
};

SessionCache::SessionCache(std::size_t capacity)
    : impl_(std::make_unique<Impl>(capacity)) {}

SessionCache::~SessionCache() = default;

void* SessionCache::take(const std::string& key) {
    std::lock_guard<std::mutex> guard(impl_->mutex);
    auto found = impl_->entries.find(key);
    if (found == impl_->entries.end()) return nullptr;
    impl_->order.splice(impl_->order.begin(), impl_->order,
                        found->second.position);
    found->second.position = impl_->order.begin();
    // The caller gets its own reference; the cache keeps its entry so a second
    // concurrent connection can resume from the same session.
    SSL_SESSION* session = found->second.session;
    ::SSL_SESSION_up_ref(session);
    return session;
}

void SessionCache::store(const std::string& key, void* session) {
    auto* fresh = static_cast<SSL_SESSION*>(session);
    std::lock_guard<std::mutex> guard(impl_->mutex);

    if (auto found = impl_->entries.find(key); found != impl_->entries.end()) {
        ::SSL_SESSION_free(found->second.session);
        found->second.session = fresh;
        impl_->order.splice(impl_->order.begin(), impl_->order,
                            found->second.position);
        found->second.position = impl_->order.begin();
        return;
    }

    if (impl_->entries.size() >= impl_->capacity && !impl_->order.empty()) {
        const std::string& oldest = impl_->order.back();
        if (auto stale = impl_->entries.find(oldest);
            stale != impl_->entries.end()) {
            ::SSL_SESSION_free(stale->second.session);
            impl_->entries.erase(stale);
        }
        impl_->order.pop_back();
    }

    impl_->order.push_front(key);
    impl_->entries.emplace(key, SessionCache::Impl::Entry{
                                    fresh, impl_->order.begin()});
}

std::shared_ptr<SessionCache> new_lru_client_session_cache(
    std::size_t capacity) {
    return std::make_shared<SessionCache>(capacity);
}

// The BIO pair keeps OpenSSL entirely in memory: it never touches the
// descriptor itself. Ciphertext moves between the network BIO and the socket
// here, which is what lets every wait go through the runtime's reactor instead
// of a blocking socket callback.
struct Conn::State {
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

    // ALPN wire form: each protocol as a length-prefixed byte string, which is
    // both what a client advertises and what a server's callback compares
    // against. Built once from config.next_protos.
    std::vector<unsigned char> alpn_wire;

    // One SSL_CTX per additional server certificate for SNI. The servername
    // callback switches the live SSL onto the one whose certificate matches the
    // client's name. context above is the default (certificates[0]).
    std::vector<SSL_CTX*> sni_contexts;

    State() : transfer(16 * 1024) {}

    ~State() {
        if (ssl != nullptr) ::SSL_free(ssl);
        if (network != nullptr) ::BIO_free(network);
        for (SSL_CTX* ctx : sni_contexts) {
            if (ctx != context) ::SSL_CTX_free(ctx);
        }
        if (context != nullptr) ::SSL_CTX_free(context);
    }
};

namespace {

// Encodes next_protos into ALPN wire form: one length byte then the bytes, per
// protocol. Returns EINVAL for a name that is empty or over 255 bytes, which the
// wire format cannot represent.
Result<std::vector<unsigned char>> build_alpn_wire(
    const std::vector<std::string>& protos) {
    std::vector<unsigned char> wire;
    for (const std::string& proto : protos) {
        if (proto.empty() || proto.size() > 255) return Error{EINVAL};
        wire.push_back(static_cast<unsigned char>(proto.size()));
        wire.insert(wire.end(), proto.begin(), proto.end());
    }
    return wire;
}

// Server ALPN callback: pick the first of our protocols that the client also
// offered. OpenSSL does the intersection given both wire lists.
int alpn_select_cb(SSL*, const unsigned char** out, unsigned char* outlen,
                   const unsigned char* in, unsigned int inlen, void* arg) {
    auto* state = static_cast<Conn::State*>(arg);
    if (::SSL_select_next_proto(const_cast<unsigned char**>(out), outlen,
                                state->alpn_wire.data(),
                                static_cast<unsigned int>(state->alpn_wire.size()),
                                in, inlen) != OPENSSL_NPN_NEGOTIATED) {
        // No overlap. Declining lets the handshake continue on HTTP/1.1 rather
        // than failing it, which is what a browser expects.
        return SSL_TLSEXT_ERR_NOACK;
    }
    return SSL_TLSEXT_ERR_OK;
}

// Server SNI callback: switch to the certificate whose subjectAltNames match
// the name the client sent. No match keeps the default context.
int servername_cb(SSL* ssl, int*, void* arg) {
    auto* state = static_cast<Conn::State*>(arg);
    const char* name = ::SSL_get_servername(ssl, TLSEXT_NAMETYPE_host_name);
    if (name == nullptr || state->sni_contexts.empty()) {
        return SSL_TLSEXT_ERR_OK;
    }
    for (SSL_CTX* ctx : state->sni_contexts) {
        X509* cert = ::SSL_CTX_get0_certificate(ctx);
        // X509_check_host does the wildcard and SAN matching, so cio does not
        // parse the certificate itself.
        if (cert != nullptr &&
            ::X509_check_host(cert, name, 0, 0, nullptr) == 1) {
            ::SSL_set_SSL_CTX(ssl, ctx);
            return SSL_TLSEXT_ERR_OK;
        }
    }
    return SSL_TLSEXT_ERR_OK;  // default certificate
}

// Expands the user's ticket secret to the length this OpenSSL wants. The length
// is version-dependent — 48 bytes once, 80 now — so asking the library rather
// than hard-coding it is what keeps one configuration portable. HKDF is being
// used for exactly what it is for: turning one secret into fixed-size key
// material.
Result<std::vector<unsigned char>> derive_ticket_key(const std::string& secret,
                                                     std::size_t length) {
    std::vector<unsigned char> derived(length);
    EVP_PKEY_CTX* kdf = ::EVP_PKEY_CTX_new_id(EVP_PKEY_HKDF, nullptr);
    if (kdf == nullptr) return take_openssl_error();

    static const char kInfo[] = "cio tls session ticket";
    std::size_t out = length;
    const bool ok =
        ::EVP_PKEY_derive_init(kdf) == 1 &&
        ::EVP_PKEY_CTX_set_hkdf_md(kdf, ::EVP_sha256()) == 1 &&
        ::EVP_PKEY_CTX_set1_hkdf_key(
            kdf, reinterpret_cast<const unsigned char*>(secret.data()),
            static_cast<int>(secret.size())) == 1 &&
        ::EVP_PKEY_CTX_add1_hkdf_info(
            kdf, reinterpret_cast<const unsigned char*>(kInfo),
            static_cast<int>(sizeof(kInfo) - 1)) == 1 &&
        ::EVP_PKEY_derive(kdf, derived.data(), &out) == 1;
    ::EVP_PKEY_CTX_free(kdf);

    if (!ok || out != length) return take_openssl_error();
    return derived;
}

// A registered ex_data slot so a callback can find the connection state from
// the SSL it was handed.
int state_ex_index() {
    static const int index =
        ::SSL_get_ex_new_index(0, nullptr, nullptr, nullptr, nullptr);
    return index;
}

// Client resumption: OpenSSL hands us each session the server issues. Under
// TLS 1.3 tickets arrive after the handshake, during a later read, which is why
// a client that never reads never caches anything.
int new_session_cb(SSL* ssl, SSL_SESSION* session) {
    auto* state = static_cast<Conn::State*>(
        ::SSL_get_ex_data(ssl, state_ex_index()));
    if (state == nullptr || !state->config.session_cache) return 0;

    // Cache a copy, not the session OpenSSL handed us. Under TLS 1.3 that
    // session is also the connection's own, and tearing the connection down
    // without a clean close_notify marks it not-resumable — which would poison
    // the cached entry and silently cost every later connection its resumption.
    SSL_SESSION* copy = ::SSL_SESSION_dup(session);
    if (copy == nullptr) return 0;
    state->config.session_cache->store(state->config.server_name, copy);
    return 0;  // OpenSSL keeps its own reference; ours is the copy
}

// Translates Go's ClientAuthType into OpenSSL's verify flags.
int verify_mode_for(ClientAuth auth) {
    switch (auth) {
        case ClientAuth::none:
            return SSL_VERIFY_NONE;
        case ClientAuth::request:
            return SSL_VERIFY_PEER;
        case ClientAuth::require_any:
            // Demand a certificate but do not check who signed it.
            return SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT;
        case ClientAuth::verify_if_given:
            return SSL_VERIFY_PEER;
        case ClientAuth::require_and_verify:
            return SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT;
    }
    return SSL_VERIFY_NONE;
}

// require_any accepts a certificate it cannot chain to a trusted root, which is
// what Go's RequireAnyClientCert means; every other mode leaves OpenSSL's own
// verdict alone.
int verify_callback_allow_any(int /*preverify*/, X509_STORE_CTX*) { return 1; }

// Loads a cert+key pair into a fresh SSL_CTX sharing the base's protocol and
// ALPN settings.
Result<SSL_CTX*> make_cert_context(const Certificate& cert,
                                   Conn::State& state) {
    SSL_CTX* ctx = ::SSL_CTX_new(::TLS_server_method());
    if (ctx == nullptr) return take_openssl_error();
    if (::SSL_CTX_set_min_proto_version(ctx, TLS1_2_VERSION) != 1 ||
        ::SSL_CTX_use_certificate_chain_file(
            ctx, cert.certificate_file.c_str()) != 1 ||
        ::SSL_CTX_use_PrivateKey_file(ctx, cert.private_key_file.c_str(),
                                      SSL_FILETYPE_PEM) != 1 ||
        ::SSL_CTX_check_private_key(ctx) != 1) {
        ::SSL_CTX_free(ctx);
        return take_openssl_error();
    }
    if (!state.alpn_wire.empty()) {
        ::SSL_CTX_set_alpn_select_cb(ctx, alpn_select_cb, &state);
    }
    return ctx;
}

Result<void> configure(Conn::State& state) {
    if (state.configured) return ok();
    ensure_initialized();

    if (auto wire = build_alpn_wire(state.config.next_protos); wire) {
        state.alpn_wire = std::move(*wire);
    } else {
        return wire.error();
    }

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

        // A client certificate, presented only if the server asks for one.
        const std::string& client_cert =
            config.certificates.empty() ? config.certificate_file
                                        : config.certificates.front().certificate_file;
        const std::string& client_key =
            config.certificates.empty() ? config.private_key_file
                                        : config.certificates.front().private_key_file;
        if (!client_cert.empty() && !client_key.empty()) {
            if (::SSL_CTX_use_certificate_chain_file(
                    state.context, client_cert.c_str()) != 1 ||
                ::SSL_CTX_use_PrivateKey_file(state.context, client_key.c_str(),
                                              SSL_FILETYPE_PEM) != 1 ||
                ::SSL_CTX_check_private_key(state.context) != 1) {
                return take_openssl_error();
            }
        }

        // Resumption. NO_INTERNAL_STORE keeps OpenSSL from also caching behind
        // our back, so the shared cache is the only place sessions live.
        if (config.session_cache) {
            ::SSL_CTX_set_session_cache_mode(
                state.context,
                SSL_SESS_CACHE_CLIENT | SSL_SESS_CACHE_NO_INTERNAL_STORE);
            ::SSL_CTX_sess_set_new_cb(state.context, new_session_cb);
        }
        // A client advertises its ALPN protocols directly on the SSL later; the
        // wire list is built above and applied after SSL_new.
    } else {
        const Config& config = state.config;

        // The default certificate is certificates[0] if present, else the
        // single certificate_file/private_key_file pair.
        std::vector<Certificate> certs = config.certificates;
        if (certs.empty()) {
            if (config.certificate_file.empty() ||
                config.private_key_file.empty()) {
                return Error{EINVAL};
            }
            certs.push_back({config.certificate_file, config.private_key_file});
        }

        if (::SSL_CTX_use_certificate_chain_file(
                state.context, certs.front().certificate_file.c_str()) != 1 ||
            ::SSL_CTX_use_PrivateKey_file(
                state.context, certs.front().private_key_file.c_str(),
                SSL_FILETYPE_PEM) != 1 ||
            ::SSL_CTX_check_private_key(state.context) != 1) {
            return take_openssl_error();
        }
        state.sni_contexts.push_back(state.context);

        // Additional certificates each get their own context, and the
        // servername callback selects among all of them by SNI.
        for (std::size_t i = 1; i < certs.size(); ++i) {
            auto ctx = make_cert_context(certs[i], state);
            if (!ctx) return ctx.error();
            state.sni_contexts.push_back(*ctx);
        }
        if (certs.size() > 1) {
            ::SSL_CTX_set_tlsext_servername_callback(state.context,
                                                     servername_cb);
            ::SSL_CTX_set_tlsext_servername_arg(state.context, &state);
        }

        if (!state.alpn_wire.empty()) {
            ::SSL_CTX_set_alpn_select_cb(state.context, alpn_select_cb, &state);
        }

        // Client certificates.
        if (config.client_auth != ClientAuth::none) {
            const int mode = verify_mode_for(config.client_auth);
            if (config.client_auth == ClientAuth::require_any) {
                ::SSL_CTX_set_verify(state.context, mode,
                                     verify_callback_allow_any);
            } else {
                ::SSL_CTX_set_verify(state.context, mode, nullptr);
            }
            if (config.client_ca_file.empty()) {
                if (::SSL_CTX_set_default_verify_paths(state.context) != 1) {
                    return take_openssl_error();
                }
            } else {
                if (::SSL_CTX_load_verify_locations(
                        state.context, config.client_ca_file.c_str(),
                        nullptr) != 1) {
                    return take_openssl_error();
                }
                // Advertising the acceptable CA names is what lets a client
                // pick the right certificate instead of guessing.
                STACK_OF(X509_NAME)* names =
                    ::SSL_load_client_CA_file(config.client_ca_file.c_str());
                if (names == nullptr) return take_openssl_error();
                ::SSL_CTX_set_client_CA_list(state.context, names);
            }
        }

        // Session tickets.
        if (config.session_tickets_disabled) {
            ::SSL_CTX_set_options(state.context, SSL_OP_NO_TICKET);
        } else if (!config.session_ticket_key.empty()) {
            // A secret shorter than this would weaken every ticket sealed with
            // it, so it is refused rather than stretched.
            if (config.session_ticket_key.size() < 32) return Error{EINVAL};
            // Passing no buffer asks OpenSSL how much key material it wants.
            const long needed =
                ::SSL_CTX_set_tlsext_ticket_keys(state.context, nullptr, 0);
            if (needed <= 0) return take_openssl_error();
            auto derived = derive_ticket_key(config.session_ticket_key,
                                             static_cast<std::size_t>(needed));
            if (!derived) return derived.error();
            if (::SSL_CTX_set_tlsext_ticket_keys(state.context, derived->data(),
                                                 needed) != 1) {
                return take_openssl_error();
            }
        }
    }

    state.ssl = ::SSL_new(state.context);
    if (state.ssl == nullptr) return take_openssl_error();
    // Callbacks receive only the SSL, so the state has to be reachable from it.
    ::SSL_set_ex_data(state.ssl, state_ex_index(), &state);

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
        // A client offers its ALPN protocols in the ClientHello.
        if (!state.alpn_wire.empty() &&
            ::SSL_set_alpn_protos(state.ssl, state.alpn_wire.data(),
                                  static_cast<unsigned int>(
                                      state.alpn_wire.size())) != 0) {
            return take_openssl_error();
        }

        // Offer a cached session, if this endpoint has one. A server that no
        // longer accepts it simply falls back to a full handshake.
        if (state.config.session_cache) {
            auto* cached = static_cast<SSL_SESSION*>(
                state.config.session_cache->take(state.config.server_name));
            if (cached != nullptr) {
                ::SSL_set_session(state.ssl, cached);
                // set_session takes its own reference.
                ::SSL_SESSION_free(cached);
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
Task<Result<void>> flush_network(Conn::State& state) {
    for (;;) {
        // BIO_pending is a macro, so it cannot be qualified.
        const int pending = BIO_pending(state.network);
        if (pending <= 0) co_return ok();

        const std::size_t want =
            std::min(static_cast<std::size_t>(pending), state.transfer.size());
        const int read = ::BIO_read(state.network, state.transfer.data(),
                                    static_cast<int>(want));
        if (read <= 0) co_return take_openssl_error();

        // socket.write() follows the full-write contract, so one call drains
        // the batch or reports why it could not.
        auto written = co_await state.socket.write(
            std::span<const std::byte>{state.transfer.data(),
                                       static_cast<std::size_t>(read)});
        if (!written) co_return written.error();
    }
}

// Pulls one batch of ciphertext from the socket into OpenSSL. Reports
// Errc::closed at EOF so a truncated stream is not mistaken for success.
Task<Result<void>> fill_network(Conn::State& state) {
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
Task<Result<int>> pump(Conn::State& state, Attempt attempt) {
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

Conn::~Conn() = default;
Conn::Conn(Conn&&) noexcept = default;
Conn& Conn::operator=(Conn&&) noexcept = default;

Conn client(net::TcpConn stream, Config config) {
    Conn tls;
    tls.state_ = std::make_unique<Conn::State>();
    tls.state_->socket = std::move(stream);
    tls.state_->config = std::move(config);
    tls.state_->is_client = true;
    return tls;
}

Conn server(net::TcpConn stream, Config config) {
    Conn tls;
    tls.state_ = std::make_unique<Conn::State>();
    tls.state_->socket = std::move(stream);
    tls.state_->config = std::move(config);
    tls.state_->is_client = false;
    return tls;
}

Task<Result<void>> Conn::handshake() {
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

Task<Result<std::size_t>> Conn::read(std::span<std::byte> buffer) {
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

Task<Result<std::size_t>> Conn::write(std::span<const std::byte> buffer) {
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

Task<Result<void>> Conn::shutdown() {
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

Result<net::SocketAddr> Conn::local_addr() const {
    if (state_ == nullptr) return Error{EBADF};
    return state_->socket.local_addr();
}

Result<net::SocketAddr> Conn::remote_addr() const {
    if (state_ == nullptr) return Error{EBADF};
    return state_->socket.remote_addr();
}

void Conn::set_deadline(TimePoint deadline) {
    if (state_ != nullptr) state_->socket.set_deadline(deadline);
}

void Conn::set_read_deadline(TimePoint deadline) {
    if (state_ != nullptr) state_->socket.set_read_deadline(deadline);
}

void Conn::set_write_deadline(TimePoint deadline) {
    if (state_ != nullptr) state_->socket.set_write_deadline(deadline);
}

void Conn::clear_deadline() {
    if (state_ != nullptr) state_->socket.clear_deadline();
}

void Conn::clear_read_deadline() {
    if (state_ != nullptr) state_->socket.clear_read_deadline();
}

void Conn::clear_write_deadline() {
    if (state_ != nullptr) state_->socket.clear_write_deadline();
}

void Conn::set_cancel(CancelToken token) {
    if (state_ != nullptr) state_->socket.set_cancel(std::move(token));
}

void Conn::clear_cancel() {
    if (state_ != nullptr) state_->socket.clear_cancel();
}

void Conn::close() {
    if (state_ != nullptr) state_->socket.close();
}

std::string Conn::protocol_version() const {
    if (state_ == nullptr || state_->ssl == nullptr) return {};
    const char* name = ::SSL_get_version(state_->ssl);
    return name != nullptr ? std::string(name) : std::string{};
}

std::string Conn::negotiated_protocol() const {
    if (state_ == nullptr || state_->ssl == nullptr) return {};
    const unsigned char* proto = nullptr;
    unsigned int len = 0;
    ::SSL_get0_alpn_selected(state_->ssl, &proto, &len);
    return proto != nullptr
               ? std::string(reinterpret_cast<const char*>(proto), len)
               : std::string{};
}

bool Conn::did_resume() const {
    if (state_ == nullptr || state_->ssl == nullptr) return false;
    return ::SSL_session_reused(state_->ssl) == 1;
}

std::string Conn::peer_certificate_subject() const {
    if (state_ == nullptr || state_->ssl == nullptr) return {};
    X509* cert = ::SSL_get1_peer_certificate(state_->ssl);
    if (cert == nullptr) return {};
    char buffer[512];
    const char* line =
        ::X509_NAME_oneline(::X509_get_subject_name(cert), buffer,
                            static_cast<int>(sizeof(buffer)));
    std::string subject = line != nullptr ? std::string(line) : std::string{};
    ::X509_free(cert);
    return subject;
}

std::string Conn::server_name() const {
    if (state_ == nullptr || state_->ssl == nullptr) return {};
    const char* name =
        ::SSL_get_servername(state_->ssl, TLSEXT_NAMETYPE_host_name);
    return name != nullptr ? std::string(name) : std::string{};
}

}  // namespace cio::tls
