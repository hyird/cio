#include "cio/fs.hpp"

#include <fcntl.h>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <string_view>

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

Task<Result<std::vector<DirEntry>>> read_dir(std::string path) {
    co_return co_await on_pool(
        [path = std::move(path)]() -> Result<std::vector<DirEntry>> {
            DIR* dir = ::opendir(path.c_str());
            if (dir == nullptr) return Error::from_errno();

            std::vector<DirEntry> entries;
            errno = 0;
            for (dirent* e = ::readdir(dir); e != nullptr; e = ::readdir(dir)) {
                const std::string_view name(e->d_name);
                if (name == "." || name == "..") continue;

                DirEntry entry;
                entry.name = std::string(name);
                // d_type is not filled in on every filesystem; fall back to
                // lstat rather than reporting everything as unknown.
                switch (e->d_type) {
                    case DT_DIR: entry.is_dir = true; break;
                    case DT_REG: entry.is_regular = true; break;
                    case DT_LNK: entry.is_symlink = true; break;
                    default: {
                        struct ::stat st {};
                        const std::string full = path + "/" + entry.name;
                        if (::lstat(full.c_str(), &st) == 0) {
                            entry.is_dir = S_ISDIR(st.st_mode);
                            entry.is_regular = S_ISREG(st.st_mode);
                            entry.is_symlink = S_ISLNK(st.st_mode);
                        }
                        break;
                    }
                }
                entries.push_back(std::move(entry));
                errno = 0;
            }
            const int read_error = errno;
            ::closedir(dir);
            // readdir returns null both at the end and on failure; errno is the
            // only way to tell a short listing from a complete one.
            if (read_error != 0) return Error{read_error};
            return entries;
        });
}

Task<Result<void>> rename(std::string from, std::string to) {
    co_return co_await on_pool(
        [from = std::move(from), to = std::move(to)]() -> Result<void> {
            if (::rename(from.c_str(), to.c_str()) != 0) {
                return Error::from_errno();
            }
            return ok();
        });
}

Task<Result<void>> mkdir(std::string path, std::uint32_t permissions) {
    co_return co_await on_pool(
        [path = std::move(path), permissions]() -> Result<void> {
            if (::mkdir(path.c_str(), static_cast<mode_t>(permissions)) != 0) {
                return Error::from_errno();
            }
            return ok();
        });
}

Task<Result<void>> mkdir_all(std::string path, std::uint32_t permissions) {
    co_return co_await on_pool(
        [path = std::move(path), permissions]() -> Result<void> {
            if (path.empty()) return Error{EINVAL};

            // Create each prefix in turn. An existing directory is success, as
            // os.MkdirAll defines it; an existing *file* is not.
            std::string partial;
            partial.reserve(path.size());
            std::size_t at = 0;
            while (at <= path.size()) {
                const std::size_t slash = path.find('/', at);
                const std::size_t end =
                    slash == std::string::npos ? path.size() : slash;
                partial.assign(path, 0, end == 0 ? 1 : end);
                at = end + 1;
                if (partial == "/" || partial.empty()) {
                    if (slash == std::string::npos) break;
                    continue;
                }
                if (::mkdir(partial.c_str(),
                            static_cast<mode_t>(permissions)) != 0) {
                    const int error = errno;
                    if (error != EEXIST) return Error{error};
                    struct ::stat st {};
                    if (::stat(partial.c_str(), &st) != 0) {
                        return Error::from_errno();
                    }
                    if (!S_ISDIR(st.st_mode)) return Error{ENOTDIR};
                }
                if (slash == std::string::npos) break;
            }
            return ok();
        });
}

Task<Result<void>> remove_all(std::string path) {
    // Recursion happens on the pool thread in one job: walking a tree with a
    // job per entry would multiply admission pressure for no benefit.
    co_return co_await on_pool(
        [path = std::move(path)]() -> Result<void> {
            struct Walker {
                static Result<void> remove(const std::string& target) {
                    struct ::stat st {};
                    if (::lstat(target.c_str(), &st) != 0) {
                        const int error = errno;
                        // Already gone is success, as os.RemoveAll defines it.
                        if (error == ENOENT) return ok();
                        return Error{error};
                    }
                    // A symlink is removed, never followed: following one would
                    // let a link inside the tree delete something outside it.
                    if (S_ISDIR(st.st_mode)) {
                        DIR* dir = ::opendir(target.c_str());
                        if (dir == nullptr) return Error::from_errno();
                        Result<void> failure = ok();
                        errno = 0;
                        for (dirent* e = ::readdir(dir); e != nullptr;
                             e = ::readdir(dir)) {
                            const std::string_view name(e->d_name);
                            if (name == "." || name == "..") continue;
                            auto child =
                                remove(target + "/" + std::string(name));
                            if (!child && failure) failure = child;
                            errno = 0;
                        }
                        const int read_error = errno;
                        ::closedir(dir);
                        if (!failure) return failure;
                        if (read_error != 0) return Error{read_error};
                        if (::rmdir(target.c_str()) != 0) {
                            return Error::from_errno();
                        }
                        return ok();
                    }
                    if (::unlink(target.c_str()) != 0) {
                        const int error = errno;
                        if (error == ENOENT) return ok();
                        return Error{error};
                    }
                    return ok();
                }
            };
            return Walker::remove(path);
        });
}

Task<Result<std::vector<std::byte>>> read_file(std::string path,
                                              std::size_t limit) {
    co_return co_await on_pool(
        [path = std::move(path), limit]() -> Result<std::vector<std::byte>> {
            const int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
            if (fd < 0) return Error::from_errno();

            std::vector<std::byte> out;
            // Size the buffer from stat as a hint only; a file can grow between
            // the stat and the reads, so the limit is what actually bounds this.
            struct ::stat st {};
            if (::fstat(fd, &st) == 0 && S_ISREG(st.st_mode) &&
                st.st_size > 0) {
                const auto hint = static_cast<std::size_t>(st.st_size);
                out.reserve(std::min(hint, limit));
            }

            std::byte chunk[64 * 1024];
            for (;;) {
                const ssize_t n = ::read(fd, chunk, sizeof(chunk));
                if (n < 0) {
                    const int error = errno;
                    if (error == EINTR) continue;
                    ::close(fd);
                    return Error{error};
                }
                if (n == 0) break;
                if (out.size() + static_cast<std::size_t>(n) > limit) {
                    ::close(fd);
                    return Error{EMSGSIZE};
                }
                out.insert(out.end(), chunk, chunk + n);
            }
            ::close(fd);
            return out;
        });
}

Task<Result<void>> write_file(std::string path,
                             std::span<const std::byte> contents,
                             std::uint32_t permissions) {
    // The span must stay alive until the job finishes, which it does: this task
    // does not resume until the pool call returns, and file operations are not
    // cancellable for exactly that reason.
    co_return co_await on_pool(
        [path = std::move(path), contents, permissions]() -> Result<void> {
            const int fd = ::open(path.c_str(),
                                  O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC,
                                  static_cast<mode_t>(permissions));
            if (fd < 0) return Error::from_errno();

            std::size_t written = 0;
            while (written < contents.size()) {
                const ssize_t n = ::write(fd, contents.data() + written,
                                          contents.size() - written);
                if (n < 0) {
                    const int error = errno;
                    if (error == EINTR) continue;
                    ::close(fd);
                    return Error{error};
                }
                if (n == 0) {
                    ::close(fd);
                    return Error{ENOSPC};
                }
                written += static_cast<std::size_t>(n);
            }
            if (::close(fd) != 0) return Error::from_errno();
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
