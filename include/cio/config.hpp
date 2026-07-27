// cio — a goroutine-style stackless coroutine runtime for C++20.
//
// Platform detection, attributes and tunables. Nothing in here is part of the
// public API surface users are meant to touch.
#pragma once

#include <cstddef>
#include <cstdint>
#include <new>

// Linux only, deliberately. A portability layer that is never compiled on the
// other platforms is a liability, not a feature: it rots silently and invites
// people to trust it. The reactor's backend-independent half is kept separate
// (src/reactor_common.cpp) so a kqueue or IOCP backend has a clean place to go,
// but no such backend is claimed here.
#if defined(__linux__)
#  define CIO_LINUX 1
#  define CIO_REACTOR_EPOLL 1
#else
#  error "cio requires Linux (epoll). See src/reactor_common.cpp for the backend seam."
#endif

#if defined(__GNUC__) || defined(__clang__)
#  define CIO_LIKELY(x)   __builtin_expect(!!(x), 1)
#  define CIO_UNLIKELY(x) __builtin_expect(!!(x), 0)
#  define CIO_ALWAYS_INLINE inline __attribute__((always_inline))
#  define CIO_NOINLINE __attribute__((noinline))
#else
#  define CIO_LIKELY(x)   (x)
#  define CIO_UNLIKELY(x) (x)
#  define CIO_ALWAYS_INLINE inline
#  define CIO_NOINLINE
#endif

namespace cio {

// Destructive interference size. Hardcoded rather than using
// std::hardware_destructive_interference_size because that macro makes the ABI
// depend on the compiler's tuning flags.
inline constexpr std::size_t kCacheLine = 64;

}  // namespace cio

#define CIO_CACHE_ALIGNED alignas(::cio::kCacheLine)

namespace cio::detail {

// Per-worker bounded run queue capacity. Must be a power of two.
inline constexpr std::uint32_t kLocalQueueCapacity = 256;

// How often (in scheduler iterations) a worker forcibly checks the global run
// queue, so that globally-queued tasks cannot be starved by a hot local pair.
inline constexpr std::uint32_t kGlobalQueueInterval = 61;

}  // namespace cio::detail
