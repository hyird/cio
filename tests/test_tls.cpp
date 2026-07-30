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
bool write_self_signed(const std::string& cert_path,
                       const std::string& key_path) {
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
        reinterpret_cast<const unsigned char*>("localhost"), -1, -1, 0);
    ::X509_set_issuer_name(cert, name);

    // Verification matches on subjectAltName, so the CN alone is not enough.
    X509_EXTENSION* alt = ::X509V3_EXT_conf_nid(
        nullptr, nullptr, NID_subject_alt_name, "DNS:localhost,IP:127.0.0.1");
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
};

void test_handshake_and_round_trip() {
    Certificate certificate;
    CIO_CHECK(certificate.ready);

    auto body = [&certificate]() -> cio::Task<bool> {
        auto listener = net::TcpListener::listen(net::SocketAddr::loopback_v4(0));
        CIO_CHECK(listener.has_value());
        const auto addr = listener->local_addr().value();

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
                        co_await cio::io::write_all(stream, bytes_of("world"));
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

        auto sent = co_await cio::io::write_all(stream, bytes_of("hello"));
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
        const auto addr = listener->local_addr().value();

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
        const auto addr = listener->local_addr().value();

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
                    co_await cio::io::write_all(stream, bytes_of(payload));
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

int main() {
    RUN_TEST(test_handshake_and_round_trip);
    RUN_TEST(test_verification_rejects_untrusted_certificate);
    RUN_TEST(test_large_transfer_spans_records);
    return cio_test::summary();
}
