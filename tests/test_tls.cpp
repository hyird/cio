#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/x509v3.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <vector>

#include "cio/cio.hpp"
#include "cio/tls.hpp"
#include "test_util.hpp"

using namespace std::chrono_literals;
namespace net = cio::net;

namespace {

std::span<const std::byte> bytes_of(std::string_view s) {
    return {reinterpret_cast<const std::byte*>(s.data()), s.size()};
}

std::string string_of(std::span<const std::byte> b) {
    return std::string(reinterpret_cast<const char*>(b.data()), b.size());
}

class TempFile {
public:
    TempFile() {
        const char* base = std::getenv("TMPDIR");
        path_ = std::string(base != nullptr ? base : "/tmp") + "/cio_tls_" +
                std::to_string(::getpid()) + "_" +
                std::to_string(counter_.fetch_add(1));
    }
    ~TempFile() { ::unlink(path_.c_str()); }
    TempFile(const TempFile&) = delete;
    TempFile& operator=(const TempFile&) = delete;
    const std::string& get() const noexcept { return path_; }

private:
    std::string path_;
    static std::atomic<unsigned> counter_;
};

std::atomic<unsigned> TempFile::counter_{0};

// A self-signed certificate for "localhost", written to PEM files so the
// public config surface (which takes paths) is what the test exercises.
bool write_self_signed(const std::string& cert_path, const std::string& key_path,
                       const std::string& cn = "localhost",
                       const std::string& san = "DNS:localhost,IP:127.0.0.1") {
    EVP_PKEY* key = ::EVP_RSA_gen(2048);
    if (key == nullptr) return false;

    X509* cert = ::X509_new();
    if (cert == nullptr) {
        ::EVP_PKEY_free(key);
        return false;
    }

    ::ASN1_INTEGER_set(::X509_get_serialNumber(cert), 1);
    ::X509_gmtime_adj(::X509_getm_notBefore(cert), 0);
    ::X509_gmtime_adj(::X509_getm_notAfter(cert), 3600);
    ::X509_set_pubkey(cert, key);

    X509_NAME* name = ::X509_get_subject_name(cert);
    ::X509_NAME_add_entry_by_txt(
        name, "CN", MBSTRING_ASC,
        reinterpret_cast<const unsigned char*>(cn.c_str()), -1, -1, 0);
    ::X509_set_issuer_name(cert, name);

    // Verification matches on subjectAltName, so the CN alone is not enough.
    X509_EXTENSION* alt = ::X509V3_EXT_conf_nid(nullptr, nullptr,
                                                NID_subject_alt_name, san.c_str());
    if (alt != nullptr) {
        ::X509_add_ext(cert, alt, -1);
        ::X509_EXTENSION_free(alt);
    }

    bool ok = ::X509_sign(cert, key, ::EVP_sha256()) != 0;

    if (ok) {
        FILE* cert_file = std::fopen(cert_path.c_str(), "wb");
        ok = cert_file != nullptr && ::PEM_write_X509(cert_file, cert) == 1;
        if (cert_file != nullptr) std::fclose(cert_file);
    }
    if (ok) {
        FILE* key_file = std::fopen(key_path.c_str(), "wb");
        ok = key_file != nullptr &&
             ::PEM_write_PrivateKey(key_file, key, nullptr, nullptr, 0, nullptr,
                                    nullptr) == 1;
        if (key_file != nullptr) std::fclose(key_file);
    }

    ::X509_free(cert);
    ::EVP_PKEY_free(key);
    return ok;
}

struct Certificate {
    TempFile cert;
    TempFile key;
    bool ready = false;

    Certificate() { ready = write_self_signed(cert.get(), key.get()); }
    // A certificate for a specific name, for the SNI test.
    explicit Certificate(const std::string& name) {
        ready = write_self_signed(cert.get(), key.get(), name, "DNS:" + name);
    }
};

void test_handshake_and_round_trip() {
    Certificate certificate;
    CIO_CHECK(certificate.ready);

    auto body = [&certificate]() -> cio::Task<bool> {
        auto listener = net::TcpListener::listen(net::SocketAddr::loopback_v4(0));
        CIO_CHECK(listener.has_value());
        const auto addr = listener->addr().value();

        auto serving = cio::spawn(
            [](net::TcpListener l, std::string cert,
               std::string key) -> cio::Task<std::string> {
                auto conn = co_await l.accept();
                if (!conn) co_return std::string{"accept failed"};

                cio::tls::Config config;
                config.certificate_file = std::move(cert);
                config.private_key_file = std::move(key);
                auto stream = cio::tls::server(std::move(*conn), config);

                if (auto shaken = co_await stream.handshake(); !shaken) {
                    co_return std::string{"server handshake failed"};
                }

                std::vector<std::byte> buffer(5);
                if (auto filled = co_await cio::io::read_full(
                        stream, std::span<std::byte>{buffer});
                    !filled) {
                    co_return std::string{"server read failed"};
                }
                if (auto written =
                        co_await stream.write(bytes_of("world"));
                    !written) {
                    co_return std::string{"server write failed"};
                }
                (void)co_await stream.shutdown();
                co_return string_of(buffer);
            }(std::move(*listener), certificate.cert.get(),
              certificate.key.get()));

        auto tcp = co_await net::TcpConn::dial(addr);
        CIO_CHECK(tcp.has_value());

        cio::tls::Config config;
        config.server_name = "localhost";
        config.ca_file = certificate.cert.get();  // trust our own CA
        auto stream = cio::tls::client(std::move(*tcp), config);

        auto shaken = co_await stream.handshake();
        CIO_CHECK(shaken.has_value());
        CIO_CHECK(!stream.protocol_version().empty());

        auto sent = co_await stream.write(bytes_of("hello"));
        CIO_CHECK(sent.has_value());

        std::vector<std::byte> reply(5);
        auto filled = co_await cio::io::read_full(stream, std::span<std::byte>{reply});
        CIO_CHECK(filled.has_value());
        CIO_CHECK_EQ(string_of(reply), std::string("world"));

        // A clean close_notify surfaces as end of stream, not an error.
        std::vector<std::byte> after(1);
        auto eof = co_await stream.read(after);
        CIO_CHECK(eof.has_value());
        CIO_CHECK_EQ(*eof, std::size_t{0});

        (void)co_await stream.shutdown();
        CIO_CHECK_EQ(co_await serving, std::string("hello"));
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

// An untrusted certificate must fail the handshake rather than connect
// unauthenticated.
void test_verification_rejects_untrusted_certificate() {
    Certificate certificate;
    CIO_CHECK(certificate.ready);

    auto body = [&certificate]() -> cio::Task<bool> {
        auto listener = net::TcpListener::listen(net::SocketAddr::loopback_v4(0));
        CIO_CHECK(listener.has_value());
        const auto addr = listener->addr().value();

        auto serving = cio::spawn(
            [](net::TcpListener l, std::string cert,
               std::string key) -> cio::Task<> {
                auto conn = co_await l.accept();
                if (!conn) co_return;
                cio::tls::Config config;
                config.certificate_file = std::move(cert);
                config.private_key_file = std::move(key);
                auto stream = cio::tls::server(std::move(*conn), config);
                // Expected to fail: the client rejects us and hangs up.
                (void)co_await stream.handshake();
            }(std::move(*listener), certificate.cert.get(),
              certificate.key.get()));

        auto tcp = co_await net::TcpConn::dial(addr);
        CIO_CHECK(tcp.has_value());

        cio::tls::Config config;
        config.server_name = "localhost";
        config.verify_peer = true;  // system trust store; our cert is not in it
        auto stream = cio::tls::client(std::move(*tcp), config);

        auto shaken = co_await stream.handshake();
        CIO_CHECK(!shaken.has_value());

        stream.close();
        co_await serving;
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

// Larger than one TLS record, so the record layer has to reassemble across
// several socket reads.
void test_large_transfer_spans_records() {
    Certificate certificate;
    CIO_CHECK(certificate.ready);

    auto body = [&certificate]() -> cio::Task<bool> {
        auto listener = net::TcpListener::listen(net::SocketAddr::loopback_v4(0));
        CIO_CHECK(listener.has_value());
        const auto addr = listener->addr().value();

        constexpr std::size_t kSize = 256 * 1024;

        auto serving = cio::spawn(
            [](net::TcpListener l, std::string cert,
               std::string key) -> cio::Task<bool> {
                auto conn = co_await l.accept();
                if (!conn) co_return false;
                cio::tls::Config config;
                config.certificate_file = std::move(cert);
                config.private_key_file = std::move(key);
                auto stream = cio::tls::server(std::move(*conn), config);
                if (auto shaken = co_await stream.handshake(); !shaken) {
                    co_return false;
                }

                const std::string payload(kSize, 'z');
                const auto written =
                    co_await stream.write(bytes_of(payload));
                (void)co_await stream.shutdown();
                co_return written.has_value();
            }(std::move(*listener), certificate.cert.get(),
              certificate.key.get()));

        auto tcp = co_await net::TcpConn::dial(addr);
        CIO_CHECK(tcp.has_value());

        cio::tls::Config config;
        config.server_name = "localhost";
        config.ca_file = certificate.cert.get();
        auto stream = cio::tls::client(std::move(*tcp), config);
        CIO_CHECK((co_await stream.handshake()).has_value());

        std::vector<std::byte> received(kSize);
        auto filled =
            co_await cio::io::read_full(stream, std::span<std::byte>{received});
        CIO_CHECK(filled.has_value());
        CIO_CHECK_EQ(static_cast<char>(received.front()), 'z');
        CIO_CHECK_EQ(static_cast<char>(received.back()), 'z');

        CIO_CHECK(co_await serving);
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

}  // namespace

// ALPN: the h2 negotiation an HTTP/2 server depends on. Both ends offer a list;
// the server picks the client's highest that it also supports.
void test_alpn_negotiates_h2() {
    Certificate certificate;
    CIO_CHECK(certificate.ready);

    auto body = [&certificate]() -> cio::Task<bool> {
        auto listener = net::TcpListener::listen(net::SocketAddr::loopback_v4(0));
        CIO_CHECK(listener.has_value());
        const auto addr = listener->addr().value();

        auto serving = cio::spawn(
            [](net::TcpListener l, std::string cert,
               std::string key) -> cio::Task<std::string> {
                auto conn = co_await l.accept();
                if (!conn) co_return std::string{"accept failed"};
                cio::tls::Config config;
                config.certificate_file = std::move(cert);
                config.private_key_file = std::move(key);
                config.next_protos = {"h2", "http/1.1"};
                auto stream = cio::tls::server(std::move(*conn), config);
                if (auto shaken = co_await stream.handshake(); !shaken) {
                    co_return std::string{"server handshake failed"};
                }
                // The server dispatches its protocol stack on this.
                co_return stream.negotiated_protocol();
            }(std::move(*listener), certificate.cert.get(),
              certificate.key.get()));

        auto tcp = co_await net::TcpConn::dial(addr);
        CIO_CHECK(tcp.has_value());
        cio::tls::Config config;
        config.server_name = "localhost";
        config.ca_file = certificate.cert.get();
        // The client prefers http/1.1 but offers h2; the server prefers h2, and
        // the server's preference wins, as in Go and OpenSSL.
        config.next_protos = {"http/1.1", "h2"};
        auto stream = cio::tls::client(std::move(*tcp), config);
        auto shaken = co_await stream.handshake();
        CIO_CHECK(shaken.has_value());

        CIO_CHECK_EQ(stream.negotiated_protocol(), std::string("h2"));
        CIO_CHECK_EQ(co_await serving, std::string("h2"));
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

// No overlap must not fail the handshake — the connection continues with no
// negotiated protocol, which a server treats as HTTP/1.1.
void test_alpn_no_overlap_is_not_fatal() {
    Certificate certificate;
    CIO_CHECK(certificate.ready);

    auto body = [&certificate]() -> cio::Task<bool> {
        auto listener = net::TcpListener::listen(net::SocketAddr::loopback_v4(0));
        CIO_CHECK(listener.has_value());
        const auto addr = listener->addr().value();

        auto serving = cio::spawn(
            [](net::TcpListener l, std::string cert,
               std::string key) -> cio::Task<bool> {
                auto conn = co_await l.accept();
                if (!conn) co_return false;
                cio::tls::Config config;
                config.certificate_file = std::move(cert);
                config.private_key_file = std::move(key);
                config.next_protos = {"h2"};
                auto stream = cio::tls::server(std::move(*conn), config);
                if (auto shaken = co_await stream.handshake(); !shaken) {
                    co_return false;
                }
                co_return stream.negotiated_protocol().empty();
            }(std::move(*listener), certificate.cert.get(),
              certificate.key.get()));

        auto tcp = co_await net::TcpConn::dial(addr);
        CIO_CHECK(tcp.has_value());
        cio::tls::Config config;
        config.server_name = "localhost";
        config.ca_file = certificate.cert.get();
        config.next_protos = {"spdy/3"};  // no overlap with the server's "h2"
        auto stream = cio::tls::client(std::move(*tcp), config);
        auto shaken = co_await stream.handshake();
        CIO_CHECK(shaken.has_value());  // handshake still succeeds
        CIO_CHECK(stream.negotiated_protocol().empty());
        CIO_CHECK(co_await serving);
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

struct SniOutcome {
    bool client_ok = false;
    std::string server_saw;  // the SNI name the server observed
};

// SNI: one server, two certificates, selected by the name the client sends —
// what lets a single listener terminate many domains.
void test_sni_selects_certificate_by_name() {
    Certificate alpha("alpha.example");
    Certificate beta("beta.example");
    CIO_CHECK(alpha.ready);
    CIO_CHECK(beta.ready);

    auto connect_as = [&](const std::string& sni,
                          const std::string& ca) -> cio::Task<SniOutcome> {
        SniOutcome outcome;
        auto listener = net::TcpListener::listen(net::SocketAddr::loopback_v4(0));
        if (!listener) co_return outcome;
        const auto addr = listener->addr().value();

        auto serving = cio::spawn(
            [](net::TcpListener l, std::string ac, std::string ak,
               std::string bc, std::string bk) -> cio::Task<std::string> {
                auto conn = co_await l.accept();
                if (!conn) co_return std::string{};
                cio::tls::Config config;
                // alpha is first, so it is the default certificate; beta is
                // only ever presented if SNI selection actually runs.
                config.certificates = {{std::move(ac), std::move(ak)},
                                       {std::move(bc), std::move(bk)}};
                auto stream = cio::tls::server(std::move(*conn), config);
                // Fails on the mismatch case, when the client rejects our
                // certificate; the observed SNI name is recorded either way.
                (void)co_await stream.handshake();
                co_return stream.server_name();
            }(std::move(*listener), alpha.cert.get(), alpha.key.get(),
              beta.cert.get(), beta.key.get()));

        auto tcp = co_await net::TcpConn::dial(addr);
        if (!tcp) co_return outcome;
        cio::tls::Config config;
        config.server_name = sni;
        config.ca_file = ca;
        auto stream = cio::tls::client(std::move(*tcp), config);
        outcome.client_ok = (co_await stream.handshake()).has_value();
        // Close before joining. A client that rejected the certificate leaves
        // the server parked mid-handshake, and only the hang-up ends that wait.
        stream.close();
        outcome.server_saw = co_await serving;
        co_return outcome;
    };

    auto body = [&]() -> cio::Task<bool> {
        // The discriminating case. beta is not the default certificate, so this
        // can only verify against beta's CA if SNI selection actually switched
        // the server onto beta — a server stuck on the default fails here.
        auto to_beta = co_await connect_as("beta.example", beta.cert.get());
        CIO_CHECK(to_beta.client_ok);
        CIO_CHECK_EQ(to_beta.server_saw, std::string("beta.example"));

        // The default certificate still serves its own name.
        auto to_alpha = co_await connect_as("alpha.example", alpha.cert.get());
        CIO_CHECK(to_alpha.client_ok);
        CIO_CHECK_EQ(to_alpha.server_saw, std::string("alpha.example"));

        // Asking for beta while trusting only alpha must fail: the server
        // presents beta's certificate, which alpha's CA does not vouch for.
        auto mismatch = co_await connect_as("beta.example", alpha.cert.get());
        CIO_CHECK(!mismatch.client_ok);
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

// Mutual TLS: the server demands a client certificate and verifies it against a
// CA it trusts. A client without one must be refused.
void test_mutual_tls_requires_and_verifies_client_certificate() {
    Certificate server_cert;                  // for "localhost"
    Certificate client_cert("client.example");
    CIO_CHECK(server_cert.ready);
    CIO_CHECK(client_cert.ready);

    // present == false means the client offers no certificate at all.
    auto attempt = [&](bool present) -> cio::Task<bool> {
        auto listener = net::TcpListener::listen(net::SocketAddr::loopback_v4(0));
        if (!listener) co_return false;
        const auto addr = listener->addr().value();

        auto serving = cio::spawn(
            [](net::TcpListener l, std::string sc, std::string sk,
               std::string client_ca) -> cio::Task<std::string> {
                auto conn = co_await l.accept();
                if (!conn) co_return std::string{};
                cio::tls::Config config;
                config.certificate_file = std::move(sc);
                config.private_key_file = std::move(sk);
                config.client_auth = cio::tls::ClientAuth::require_and_verify;
                config.client_ca_file = std::move(client_ca);
                auto stream = cio::tls::server(std::move(*conn), config);
                if (auto shaken = co_await stream.handshake(); !shaken) {
                    co_return std::string{};
                }
                // The identity an authorization check would read.
                co_return stream.peer_certificate_subject();
            }(std::move(*listener), server_cert.cert.get(),
              server_cert.key.get(), client_cert.cert.get()));

        auto tcp = co_await net::TcpConn::dial(addr);
        if (!tcp) co_return false;
        cio::tls::Config config;
        config.server_name = "localhost";
        config.ca_file = server_cert.cert.get();
        if (present) {
            config.certificate_file = client_cert.cert.get();
            config.private_key_file = client_cert.key.get();
        }
        auto stream = cio::tls::client(std::move(*tcp), config);
        const bool client_ok = (co_await stream.handshake()).has_value();
        stream.close();
        const std::string subject = co_await serving;

        if (!present) co_return !client_ok || subject.empty();
        // With a certificate, both ends succeed and the server sees the name.
        co_return client_ok && subject.find("client.example") != std::string::npos;
    };

    auto body = [&]() -> cio::Task<bool> {
        CIO_CHECK(co_await attempt(true));   // presenting one works
        CIO_CHECK(co_await attempt(false));  // omitting one is refused
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

// Session resumption: a second connection sharing the cache skips the full
// handshake. The ticket key has to be shared too, because every connection
// builds its own OpenSSL context.
void test_session_resumption_across_connections() {
    Certificate certificate;
    CIO_CHECK(certificate.ready);
    auto cache = cio::tls::new_lru_client_session_cache(8);
    const std::string ticket_key(48, 'k');

    auto once = [&]() -> cio::Task<bool> {
        auto listener = net::TcpListener::listen(net::SocketAddr::loopback_v4(0));
        if (!listener) co_return false;
        const auto addr = listener->addr().value();

        auto serving = cio::spawn(
            [](net::TcpListener l, std::string cert, std::string key,
               std::string tk) -> cio::Task<> {
                auto conn = co_await l.accept();
                if (!conn) co_return;
                cio::tls::Config config;
                config.certificate_file = std::move(cert);
                config.private_key_file = std::move(key);
                config.session_ticket_key = std::move(tk);
                auto stream = cio::tls::server(std::move(*conn), config);
                if (auto sh = co_await stream.handshake(); !sh) co_return;
                // Say something, then close cleanly. TLS 1.3 delivers its
                // session ticket after the handshake, as its own record, so the
                // client only sees it by reading past the application data.
                (void)co_await stream.write(bytes_of("hi"));
                (void)co_await stream.shutdown();
            }(std::move(*listener), certificate.cert.get(),
              certificate.key.get(), ticket_key));

        auto tcp = co_await net::TcpConn::dial(addr);
        if (!tcp) co_return false;
        cio::tls::Config config;
        config.server_name = "localhost";
        config.ca_file = certificate.cert.get();
        config.session_cache = cache;
        auto stream = cio::tls::client(std::move(*tcp), config);
        if (auto shaken = co_await stream.handshake(); !shaken) co_return false;

        const bool resumed = stream.did_resume();
        // Read all the way to end of stream. Stopping at the application data
        // would leave the ticket record unread, and nothing would be cached.
        std::vector<std::byte> chunk(64);
        for (;;) {
            auto n = co_await stream.read(chunk);
            if (!n || *n == 0) break;
        }
        stream.close();
        co_await serving;
        co_return resumed;
    };

    auto body = [&]() -> cio::Task<bool> {
        // The first connection has nothing to resume from.
        CIO_CHECK(!(co_await once()));
        // The second resumes the ticket the first one cached.
        CIO_CHECK(co_await once());
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

// One server handshake with the given Config, then a clean close.
cio::Task<> serve_one(net::TcpListener* listener,
                      const cio::tls::Config* config) {
    auto conn = co_await listener->accept();
    if (!conn) co_return;
    auto stream = cio::tls::server(std::move(*conn), *config);
    if (auto shaken = co_await stream.handshake(); !shaken) co_return;
    (void)co_await stream.write(bytes_of("hi"));
    (void)co_await stream.shutdown();
}

// One client handshake: -1 could not connect, 0 full handshake, 1 resumed.
cio::Task<int> connect_one(net::SocketAddr addr,
                           const cio::tls::Config* config) {
    auto tcp = co_await net::TcpConn::dial(addr);
    if (!tcp) co_return -1;
    auto stream = cio::tls::client(std::move(*tcp), *config);
    if (auto shaken = co_await stream.handshake(); !shaken) co_return -1;
    const int resumed = stream.did_resume() ? 1 : 0;
    // Read to end of stream so the post-handshake ticket reaches the cache.
    std::vector<std::byte> chunk(64);
    for (;;) {
        auto n = co_await stream.read(chunk);
        if (!n || *n == 0) break;
    }
    stream.close();
    co_return resumed;
}

// Reusing one Config is what makes a server resume within a process: its
// connections share the context that issued the ticket. No ticket key is set
// here — needing one would mean the contexts were not actually shared.
void test_shared_config_resumes_without_a_ticket_key() {
    Certificate certificate;
    CIO_CHECK(certificate.ready);

    auto body = [&]() -> cio::Task<bool> {
        auto listener = net::TcpListener::listen(net::SocketAddr::loopback_v4(0));
        CIO_CHECK(listener.has_value());
        const auto addr = listener->addr().value();

        cio::tls::Config server_config;
        server_config.certificate_file = certificate.cert.get();
        server_config.private_key_file = certificate.key.get();

        cio::tls::Config client_config;
        client_config.server_name = "localhost";
        client_config.ca_file = certificate.cert.get();
        client_config.session_cache = cio::tls::new_lru_client_session_cache(8);

        auto first = cio::spawn(serve_one(&*listener, &server_config));
        const int fresh = co_await connect_one(addr, &client_config);
        co_await first;
        CIO_CHECK_EQ(fresh, 0);  // nothing to resume from yet

        auto second = cio::spawn(serve_one(&*listener, &server_config));
        const int again = co_await connect_one(addr, &client_config);
        co_await second;
        CIO_CHECK_EQ(again, 1);  // resumed, with no ticket key configured
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

// The certificate and trust files are read when a Config is first used, not on
// every connection — which is what keeps file I/O off the accept path. Deleting
// them after the first handshake proves it: a second connection still works.
void test_shared_config_reads_certificate_files_once() {
    Certificate certificate;
    CIO_CHECK(certificate.ready);

    auto body = [&]() -> cio::Task<bool> {
        auto listener = net::TcpListener::listen(net::SocketAddr::loopback_v4(0));
        CIO_CHECK(listener.has_value());
        const auto addr = listener->addr().value();

        cio::tls::Config server_config;
        server_config.certificate_file = certificate.cert.get();
        server_config.private_key_file = certificate.key.get();

        cio::tls::Config client_config;
        client_config.server_name = "localhost";
        client_config.ca_file = certificate.cert.get();

        auto first = cio::spawn(serve_one(&*listener, &server_config));
        CIO_CHECK_EQ(co_await connect_one(addr, &client_config), 0);
        co_await first;

        // Both sides have compiled their Config by now. Take the files away.
        CIO_CHECK_EQ(::unlink(certificate.cert.get().c_str()), 0);
        CIO_CHECK_EQ(::unlink(certificate.key.get().c_str()), 0);

        auto second = cio::spawn(serve_one(&*listener, &server_config));
        CIO_CHECK_EQ(co_await connect_one(addr, &client_config), 0);
        co_await second;
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

// The whole point of sharing a context is that many connections use it at once,
// so it has to be exercised that way — a sequential test would tell TSan
// nothing. One Config per side, many simultaneous handshakes.
void test_shared_config_under_concurrent_connections() {
    Certificate certificate;
    CIO_CHECK(certificate.ready);
    constexpr int kCount = 8;

    auto body = [&]() -> cio::Task<bool> {
        auto listener = net::TcpListener::listen(net::SocketAddr::loopback_v4(0));
        CIO_CHECK(listener.has_value());
        const auto addr = listener->addr().value();

        cio::tls::Config server_config;
        server_config.certificate_file = certificate.cert.get();
        server_config.private_key_file = certificate.key.get();
        server_config.next_protos = {"h2", "http/1.1"};

        cio::tls::Config client_config;
        client_config.server_name = "localhost";
        client_config.ca_file = certificate.cert.get();
        client_config.next_protos = {"h2"};
        client_config.session_cache = cio::tls::new_lru_client_session_cache(8);

        auto acceptor = cio::spawn(
            [](net::TcpListener* l, cio::tls::Config config,
               int count) -> cio::Task<> {
                cio::TaskGroup group;
                for (int i = 0; i < count; ++i) {
                    auto conn = co_await l->accept();
                    if (!conn) break;
                    group.spawn(
                        [](net::TcpConn c, cio::tls::Config cfg) -> cio::Task<> {
                            auto stream = cio::tls::server(std::move(c), cfg);
                            if (auto sh = co_await stream.handshake(); !sh) {
                                co_return;
                            }
                            (void)co_await stream.write(bytes_of("hi"));
                            (void)co_await stream.shutdown();
                        }(std::move(*conn), config));
                }
                co_await group.join();
            }(&*listener, server_config, kCount));

        std::atomic<int> succeeded{0};
        {
            cio::TaskGroup clients;
            for (int i = 0; i < kCount; ++i) {
                clients.spawn([](net::SocketAddr a, cio::tls::Config cfg,
                                 std::atomic<int>* ok) -> cio::Task<> {
                    if (co_await connect_one(a, &cfg) >= 0) {
                        ok->fetch_add(1, std::memory_order_relaxed);
                    }
                }(addr, client_config, &succeeded));
            }
            co_await clients.join();
        }
        co_await acceptor;

        CIO_CHECK_EQ(succeeded.load(), kCount);
        co_return true;
    };
    CIO_CHECK(cio::run(body()));
}

int main() {
    RUN_TEST(test_handshake_and_round_trip);
    RUN_TEST(test_verification_rejects_untrusted_certificate);
    RUN_TEST(test_large_transfer_spans_records);
    RUN_TEST(test_alpn_negotiates_h2);
    RUN_TEST(test_alpn_no_overlap_is_not_fatal);
    RUN_TEST(test_sni_selects_certificate_by_name);
    RUN_TEST(test_mutual_tls_requires_and_verifies_client_certificate);
    RUN_TEST(test_session_resumption_across_connections);
    RUN_TEST(test_shared_config_resumes_without_a_ticket_key);
    RUN_TEST(test_shared_config_reads_certificate_files_once);
    RUN_TEST(test_shared_config_under_concurrent_connections);
    return cio_test::summary();
}
