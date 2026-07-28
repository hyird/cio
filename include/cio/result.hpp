// Result<T> — the error channel for I/O and other fallible runtime operations.
//
// Task bodies still use exceptions for programming errors; Result is for the
// expected-failure paths (EAGAIN-adjacent syscall errors, cancellation, closed
// channels) where an exception per event would be a performance problem.
#pragma once

#include <cerrno>
#include <cstring>
#include <string>
#include <system_error>
#include <type_traits>
#include <utility>

#include "cio/config.hpp"

namespace cio {

// Runtime-level error conditions that are not errno values.
enum class Errc : int {
    ok = 0,
    cancelled = -1,       // the operation's cancellation token fired
    timed_out = -2,       // a deadline elapsed (distinct from ETIMEDOUT)
    closed = -3,          // channel/socket closed by the peer or by us
    shutdown = -4,        // the runtime is shutting down
    would_block = -5,     // only surfaced by try_* APIs
    broken = -6,          // invariant violation in a resource (poisoned mutex, etc.)
    overloaded = -7,      // a bounded runtime queue has reached its limit
};

class Error {
public:
    Error() = default;
    explicit Error(int errno_code) noexcept : code_(errno_code) {}
    Error(Errc e) noexcept : code_(static_cast<int>(e)) {}  // NOLINT(google-explicit-constructor)

    static Error from_errno() noexcept { return Error(errno); }

    // Positive codes are errno values; negative codes are cio::Errc.
    int raw() const noexcept { return code_; }
    bool is_errno() const noexcept { return code_ > 0; }
    bool is(Errc e) const noexcept { return code_ == static_cast<int>(e); }
    bool is(int errno_code) const noexcept { return code_ == errno_code; }

    explicit operator bool() const noexcept { return code_ != 0; }

    std::string message() const {
        if (code_ > 0) return std::generic_category().message(code_);
        switch (static_cast<Errc>(code_)) {
            case Errc::ok: return "ok";
            case Errc::cancelled: return "cancelled";
            case Errc::timed_out: return "timed out";
            case Errc::closed: return "closed";
            case Errc::shutdown: return "runtime shutting down";
            case Errc::would_block: return "would block";
            case Errc::broken: return "broken";
            case Errc::overloaded: return "runtime overloaded";
        }
        return "unknown error";
    }

    friend bool operator==(Error a, Error b) noexcept { return a.code_ == b.code_; }

private:
    int code_ = 0;
};

// Thrown by Result::value() and by JoinHandle when an I/O error escapes into
// exception-land (e.g. a task that ignores a Result and calls .value()).
class SystemError : public std::runtime_error {
public:
    explicit SystemError(Error e) : std::runtime_error(e.message()), err_(e) {}
    Error error() const noexcept { return err_; }

private:
    Error err_;
};

template <typename T>
class Result;

namespace detail {
[[noreturn]] void throw_system_error(Error e);
}  // namespace detail

// A value-or-Error union. Deliberately hand-rolled rather than std::variant so
// that the success path is a plain branch on a bool with no index bookkeeping,
// and so Result<void> shares the same shape.
template <typename T>
class Result {
    static_assert(!std::is_reference_v<T>, "Result<T&> is not supported");

public:
    using value_type = T;

    Result(T value) noexcept(std::is_nothrow_move_constructible_v<T>)  // NOLINT
        : ok_(true) {
        ::new (static_cast<void*>(&storage_)) T(std::move(value));
    }
    Result(Error e) noexcept : ok_(false), err_(e) {}  // NOLINT(google-explicit-constructor)
    Result(Errc e) noexcept : ok_(false), err_(e) {}   // NOLINT(google-explicit-constructor)

    Result(const Result& other) : ok_(other.ok_), err_(other.err_) {
        if (ok_) ::new (static_cast<void*>(&storage_)) T(other.ref());
    }
    Result(Result&& other) noexcept(std::is_nothrow_move_constructible_v<T>)
        : ok_(other.ok_), err_(other.err_) {
        if (ok_) ::new (static_cast<void*>(&storage_)) T(std::move(other.ref()));
    }
    Result& operator=(const Result& other) {
        if (this != &other) {
            this->~Result();
            ::new (static_cast<void*>(this)) Result(other);
        }
        return *this;
    }
    Result& operator=(Result&& other) noexcept(std::is_nothrow_move_constructible_v<T>) {
        if (this != &other) {
            this->~Result();
            ::new (static_cast<void*>(this)) Result(std::move(other));
        }
        return *this;
    }
    ~Result() {
        if (ok_) ref().~T();
    }

    bool has_value() const noexcept { return ok_; }
    explicit operator bool() const noexcept { return ok_; }

    Error error() const noexcept { return ok_ ? Error{} : err_; }

    T& operator*() & noexcept { return ref(); }
    const T& operator*() const& noexcept { return ref(); }
    T&& operator*() && noexcept { return std::move(ref()); }
    T* operator->() noexcept { return &ref(); }
    const T* operator->() const noexcept { return &ref(); }

    // Throws SystemError if this holds an error.
    T& value() & {
        if (!ok_) detail::throw_system_error(err_);
        return ref();
    }
    T&& value() && {
        if (!ok_) detail::throw_system_error(err_);
        return std::move(ref());
    }

    template <typename U>
    T value_or(U&& fallback) const& {
        return ok_ ? ref() : static_cast<T>(std::forward<U>(fallback));
    }

private:
    T& ref() noexcept { return *std::launder(reinterpret_cast<T*>(&storage_)); }
    const T& ref() const noexcept {
        return *std::launder(reinterpret_cast<const T*>(&storage_));
    }

    bool ok_;
    Error err_{};
    alignas(T) unsigned char storage_[sizeof(T)];
};

template <>
class Result<void> {
public:
    using value_type = void;

    Result() noexcept = default;
    Result(Error e) noexcept : err_(e) {}  // NOLINT(google-explicit-constructor)
    Result(Errc e) noexcept : err_(e) {}   // NOLINT(google-explicit-constructor)

    bool has_value() const noexcept { return !static_cast<bool>(err_); }
    explicit operator bool() const noexcept { return has_value(); }
    Error error() const noexcept { return err_; }

    void value() const {
        if (err_) detail::throw_system_error(err_);
    }

private:
    Error err_{};
};

inline Result<void> ok() noexcept { return Result<void>{}; }

}  // namespace cio
