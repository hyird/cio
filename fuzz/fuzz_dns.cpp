// Fuzzes the DNS wire parser, which is the code in cio that consumes the most
// untrusted input: compression-pointer chains, length fields and record counts
// all come straight off the network. The hand-written bounds checks in
// parse_response and skip_name are exactly the kind that a fuzzer finds the hole
// in. A crash here is a remote DoS, so this is the highest-value target.
#include <cstddef>
#include <cstdint>
#include <span>

#include "cio/dns.hpp"

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data,
                                      std::size_t size) {
    const std::span<const std::byte> message{
        reinterpret_cast<const std::byte*>(data), size};
    // The expected id and port are part of the input space too, so a matching
    // id does not gate the parser off from the fuzzer.
    const std::uint16_t id =
        size >= 2 ? static_cast<std::uint16_t>((data[0] << 8) | data[1]) : 0;
    (void)cio::dns::detail::parse_response(message, id, 53);
    return 0;
}
