// Fuzzes split_host_port, whose bracket/colon disambiguation is fiddly enough
// to hide an out-of-bounds read on a malformed "[::1" or a lone ":".
#include <cstddef>
#include <cstdint>
#include <string_view>

#include "cio/net.hpp"

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data,
                                      std::size_t size) {
    const std::string_view input{reinterpret_cast<const char*>(data), size};
    (void)cio::net::split_host_port(input);
    return 0;
}
