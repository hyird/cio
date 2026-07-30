// Library version, so a downstream can adapt across releases.
//
// The numbers come from CMake's project(VERSION), which is the single source of
// truth; this header is generated from version.hpp.in at configure time when the
// project is built with CMake, and the committed copy below tracks it for
// builds that just add include/ to the search path.
#pragma once

#define CIO_VERSION_MAJOR 0
#define CIO_VERSION_MINOR 2
#define CIO_VERSION_PATCH 0

// Comparable: CIO_VERSION >= CIO_VERSION_NUMBER(0, 1, 0)
#define CIO_VERSION_NUMBER(major, minor, patch) \
    ((major) * 10000 + (minor) * 100 + (patch))
#define CIO_VERSION                                                     \
    CIO_VERSION_NUMBER(CIO_VERSION_MAJOR, CIO_VERSION_MINOR,            \
                       CIO_VERSION_PATCH)

#define CIO_VERSION_STRING "0.2.0"

namespace cio {

// Runtime counterpart, for logging a version the caller did not compile against.
constexpr int version_major() noexcept { return CIO_VERSION_MAJOR; }
constexpr int version_minor() noexcept { return CIO_VERSION_MINOR; }
constexpr int version_patch() noexcept { return CIO_VERSION_PATCH; }
constexpr const char* version_string() noexcept { return CIO_VERSION_STRING; }

}  // namespace cio
