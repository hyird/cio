#include "cio/fs.hpp"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <cerrno>

#include "cio/blocking.hpp"

namespace cio::fs {
namespace {

// Every file syscall goes through the pool under the file admission class, so
// a burst of file work cannot occupy the slots reserved for name resolution.
template <typename F>
auto on_pool(F fn) {
    return detail::blocking_in_class(std::move(fn),
                                     detail::BlockingClass::file);
}

int open_flags(const OpenOptions& options) noexcept {
    int flags = 0;
    switch (options.access) {
        case Access::read_only: flags |= O_RDONLY; break;
        case Access::write_only: flags |= O_WRONLY; break;
        case Access::read_write: flags |= O_RDWR; break;
    }
    if (options.append) flags |= O_APPEND;
    if (options.create) flags |= O_CREAT;
    if (options.exclusive) flags |= O_EXCL;
    if (options.truncate) flags |= O_TRUNC;
    if (options.sync) flags |= O_SYNC;
    return flags | O_CLOEXEC;
}

FileInfo info_from_stat(const struct ::stat& st) noexcept {
    FileInfo info;
    info.size = static_cast<std::uint64_t>(st.st_size);
    info.mode = static_cast<std::uint32_t>(st.st_mode);
    info.modified =
        FileTime{std::chrono::seconds{st.st_mtim.tv_sec} +
                 std::chrono::duration_cast<FileTime::duration>(
                     std::chrono::nanoseconds{st.st_mtim.tv_nsec})};
    return info;
}

}  // namespace

bool FileInfo::is_regular() const noexcept { return S_ISREG(mode); }
bool FileInfo::is_dir() const noexcept { return S_ISDIR(mode); }

void File::close() {
    if (fd_ < 0) return;
    // EINTR from close() must not be retried on Linux: the descriptor is
    // already released, so a retry could close a descriptor another thread has
    // since opened with the same number.
    ::close(std::exchange(fd_, -1));
}

Task<Result<File>> open(std::string path, OpenOptions options) {
    auto opened = co_await on_pool(
        [path = std::move(path), options]() -> Result<int> {
            const int fd =
                ::open(path.c_str(), open_flags(options),
                       static_cast<mode_t>(options.permissions));
            if (fd < 0) return Error::from_errno();
            return fd;
        });
    if (!opened) co_return opened.error();
    co_return File{*opened};
}

Task<Result<File>> create(std::string path, std::uint32_t permissions) {
    OpenOptions options;
    options.access = Access::read_write;
    options.create = true;
    options.truncate = true;
    options.permissions = permissions;
    co_return co_await open(std::move(path), options);
}

Task<Result<FileInfo>> stat(std::string path) {
    co_return co_await on_pool(
        [path = std::move(path)]() -> Result<FileInfo> {
            struct ::stat st {};
            if (::stat(path.c_str(), &st) != 0) return Error::from_errno();
            return info_from_stat(st);
        });
}

Task<Result<void>> remove(std::string path) {
    co_return co_await on_pool(
        [path = std::move(path)]() -> Result<void> {
            if (::unlink(path.c_str()) != 0) return Error::from_errno();
            return ok();
        });
}

Task<Result<std::size_t>> File::read(std::span<std::byte> buffer) {
    if (fd_ < 0) co_return Error{EBADF};
    if (buffer.empty()) co_return std::size_t{0};
    const int fd = fd_;
    co_return co_await on_pool([fd, buffer]() -> Result<std::size_t> {
        const ssize_t n = ::read(fd, buffer.data(), buffer.size());
        if (n < 0) return Error::from_errno();
        return static_cast<std::size_t>(n);
    });
}

Task<Result<std::size_t>> File::read_at(std::span<std::byte> buffer,
                                        std::uint64_t offset) {
    if (fd_ < 0) co_return Error{EBADF};
    if (buffer.empty()) co_return std::size_t{0};
    const int fd = fd_;
    co_return co_await on_pool([fd, buffer, offset]() -> Result<std::size_t> {
        const ssize_t n = ::pread(fd, buffer.data(), buffer.size(),
                                  static_cast<off_t>(offset));
        if (n < 0) return Error::from_errno();
        return static_cast<std::size_t>(n);
    });
}

Task<Result<std::size_t>> File::write(std::span<const std::byte> buffer) {
    if (fd_ < 0) co_return Error{EBADF};
    if (buffer.empty()) co_return std::size_t{0};
    const int fd = fd_;
    co_return co_await on_pool([fd, buffer]() -> Result<std::size_t> {
        const ssize_t n = ::write(fd, buffer.data(), buffer.size());
        if (n < 0) return Error::from_errno();
        return static_cast<std::size_t>(n);
    });
}

Task<Result<std::size_t>> File::write_at(std::span<const std::byte> buffer,
                                         std::uint64_t offset) {
    if (fd_ < 0) co_return Error{EBADF};
    if (buffer.empty()) co_return std::size_t{0};
    const int fd = fd_;
    co_return co_await on_pool([fd, buffer, offset]() -> Result<std::size_t> {
        const ssize_t n = ::pwrite(fd, buffer.data(), buffer.size(),
                                   static_cast<off_t>(offset));
        if (n < 0) return Error::from_errno();
        return static_cast<std::size_t>(n);
    });
}

Task<Result<void>> File::write_all(std::span<const std::byte> buffer) {
    while (!buffer.empty()) {
        auto n = co_await write(buffer);
        if (!n) co_return n.error();
        // A regular-file write reporting zero progress would loop forever.
        if (*n == 0) co_return Error{ENOSPC};
        buffer = buffer.subspan(*n);
    }
    co_return ok();
}

Task<Result<void>> File::sync() {
    if (fd_ < 0) co_return Error{EBADF};
    const int fd = fd_;
    co_return co_await on_pool([fd]() -> Result<void> {
        if (::fsync(fd) != 0) return Error::from_errno();
        return ok();
    });
}

Task<Result<void>> File::sync_data() {
    if (fd_ < 0) co_return Error{EBADF};
    const int fd = fd_;
    co_return co_await on_pool([fd]() -> Result<void> {
        if (::fdatasync(fd) != 0) return Error::from_errno();
        return ok();
    });
}

Task<Result<void>> File::truncate(std::uint64_t size) {
    if (fd_ < 0) co_return Error{EBADF};
    const int fd = fd_;
    co_return co_await on_pool([fd, size]() -> Result<void> {
        if (::ftruncate(fd, static_cast<off_t>(size)) != 0) {
            return Error::from_errno();
        }
        return ok();
    });
}

Task<Result<FileInfo>> File::stat() {
    if (fd_ < 0) co_return Error{EBADF};
    const int fd = fd_;
    co_return co_await on_pool([fd]() -> Result<FileInfo> {
        struct ::stat st {};
        if (::fstat(fd, &st) != 0) return Error::from_errno();
        return info_from_stat(st);
    });
}

Task<Result<std::uint64_t>> File::seek(std::uint64_t offset) {
    if (fd_ < 0) co_return Error{EBADF};
    const int fd = fd_;
    co_return co_await on_pool([fd, offset]() -> Result<std::uint64_t> {
        const off_t position =
            ::lseek(fd, static_cast<off_t>(offset), SEEK_SET);
        if (position < 0) return Error::from_errno();
        return static_cast<std::uint64_t>(position);
    });
}

}  // namespace cio::fs
