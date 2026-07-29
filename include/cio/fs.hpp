// Regular-file I/O.
//
//     auto file = co_await cio::fs::open("config.toml");
//     std::byte buffer[4096];
//     auto n = co_await file->read(buffer);
//
// A regular file is never registered with epoll and this is not readiness-based
// asynchronous I/O. It is asynchronous from the task's point of view only: the
// task suspends and a blocking-executor thread performs the syscall, so a
// worker is never parked in the kernel. Concurrent file work is bounded by
// RuntimeOptions::max_file_operations.
//
// NO CANCELLATION: file operations take no CancelToken and honour no deadline.
// Returning early from a cancelled read() would let the pool thread keep
// writing into a caller-owned span after that storage was destroyed, so the API
// waits for the syscall and reports its real result rather than offering a
// cancellation it cannot keep.
//
// OWNERSHIP: File is a move-only RAII handle and close() is idempotent. A
// handle must outlive every task operating on it. read_at()/write_at() may run
// concurrently when their buffers are distinct; read()/write() share the file
// offset and must not.
#pragma once

#include <chrono>
#include <cstdint>
#include <span>
#include <string>
#include <utility>

#include "cio/result.hpp"
#include "cio/task.hpp"

namespace cio::fs {

enum class Access {
    read_only,
    write_only,
    read_write,
};

struct OpenOptions {
    Access access = Access::read_only;
    bool append = false;
    bool create = false;
    bool exclusive = false;
    bool truncate = false;
    bool sync = false;
    std::uint32_t permissions = 0644;
};

using FileTime = std::chrono::system_clock::time_point;

struct FileInfo {
    std::uint64_t size = 0;
    std::uint32_t mode = 0;
    FileTime modified{};

    bool is_regular() const noexcept;
    bool is_directory() const noexcept;
};

class File {
public:
    File() = default;
    ~File() { close(); }

    File(File&& other) noexcept : fd_(std::exchange(other.fd_, -1)) {}
    File& operator=(File&& other) noexcept {
        if (this != &other) {
            close();
            fd_ = std::exchange(other.fd_, -1);
        }
        return *this;
    }
    File(const File&) = delete;
    File& operator=(const File&) = delete;

    bool valid() const noexcept { return fd_ >= 0; }
    int native_handle() const noexcept { return fd_; }

    // Synchronous and idempotent, so RAII destruction has deterministic
    // descriptor ownership. Durable completion is requested explicitly with
    // sync(); destruction does not imply one. On a filesystem where close()
    // itself can block for an unbounded time, call it from cio::blocking().
    void close();

    // Reads at the shared file offset. 0 means end of file.
    Task<Result<std::size_t>> read(std::span<std::byte> buffer);
    // Positioned read; does not change the shared offset.
    Task<Result<std::size_t>> read_at(std::span<std::byte> buffer,
                                      std::uint64_t offset);
    Task<Result<std::size_t>> write(std::span<const std::byte> buffer);
    Task<Result<std::size_t>> write_at(std::span<const std::byte> buffer,
                                       std::uint64_t offset);

    // Loops over short writes.
    Task<Result<void>> write_all(std::span<const std::byte> buffer);

    Task<Result<void>> sync();
    Task<Result<void>> sync_data();
    Task<Result<void>> truncate(std::uint64_t size);
    Task<Result<FileInfo>> stat();

    // Repositions the shared offset, like lseek(2) with SEEK_SET.
    Task<Result<std::uint64_t>> seek(std::uint64_t offset);

private:
    friend Task<Result<File>> open(std::string path, OpenOptions options);
    explicit File(int fd) noexcept : fd_(fd) {}

    int fd_ = -1;
};

Task<Result<File>> open(std::string path, OpenOptions options = {});
Task<Result<File>> create(std::string path, std::uint32_t permissions = 0644);
Task<Result<FileInfo>> stat(std::string path);
Task<Result<void>> remove(std::string path);

}  // namespace cio::fs
