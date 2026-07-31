// Fuzzes UnixAddr::parse — the abstract-namespace '@' handling and the
// sun_path length bound are both easy to get one byte wrong.
#include <cstddef>
#include <cstdint>
#include <string_view>

#include "cio/net.hpp"

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data,
                                      std::size_t size) {
    const std::string_view input{reinterpret_cast<const char*>(data), size};
    (void)cio::net::UnixAddr::parse(input);
    return 0;
}
