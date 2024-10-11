#include "storage_region.hpp"
#ifdef __linux__
#include <linux/fs.h>
#include <sys/ioctl.h>
#endif

#include <unistd.h>

namespace hyrise {

inline void DebugAssertPageAlignment(const std::byte* data) {
  DebugAssert(reinterpret_cast<std::uintptr_t>(data) % 512 == 0,
              "Data buffer is not properly aligned to 512 byte boundary as required for direct IO: " +
                  std::to_string(reinterpret_cast<std::uintptr_t>(data) % 512));
}

static StorageRegion::Mode find_mode_or_fail(const std::filesystem::path& file_name) {
  if (std::filesystem::is_directory(file_name)) {
    return StorageRegion::Mode::FILE_PER_PAGE_SIZE;
  } else if (std::filesystem::is_block_file(file_name)) {
    return StorageRegion::Mode::BLOCK;
  } else {
    Fail("The backing file has to be either a directory or a block device: " + file_name.string());
  }
}

StorageRegion::StorageRegion(const std::filesystem::path& path)
    : _mode(find_mode_or_fail(path)),
      _file_handles(_mode == Mode::FILE_PER_PAGE_SIZE ? _open_file_handles_in_directory(path)
                                                      : _open_file_handles_block(path))

{}

StorageRegion::~StorageRegion() {
  for (const auto& file_handle : _file_handles) {
    // We do not handle errors of the close syscall, since we are already in the destructor.
    close(file_handle.fd);
    if (_mode == Mode::FILE_PER_PAGE_SIZE) {
      std::filesystem::remove(file_handle.backing_file_name);
    }
  }
}

StorageRegion::Mode StorageRegion::mode() const {
  return _mode;
}

int StorageRegion::_open_file_descriptor(const std::filesystem::path& file_name) {
  // Partially taken from
  // DuckDB: https://github.com/duckdb/duckdb/blob/60ed227816669be497fa4ba53e593d3899479c43/src/common/local_file_system.cpp
  // LevelDB: https://github.com/google/leveldb/commit/296de8d5b8e4e57bd1e46c981114dfbe58a8c4fa

  // On Linux, we use O_DIRECT to bypass the page cache. This is not supported on Mac,
  // but we use the fcntl(F_NOCACHE) call below. Other nan that, files are opened in
  // read-write mode and created if they do not exist. O_DSYNC ensures that all file
  // data and metadata is written to the disk before the syscall returns. This
  // eliminates the need for explict fsync calls after each write.
#ifdef __APPLE__
  const int flags = O_RDWR | O_CREAT | O_DSYNC;
#elif __linux__
  const int flags = O_RDWR | O_CREAT | O_DIRECT | O_DSYNC;
#endif
  const int fd = open(file_name.string().c_str(), flags, 0666);
  if (fd < 0) {
    const auto error = errno;
    close(fd);
    Fail("Error while opening backing file at " + file_name.string().c_str() + ": " + strerror(error) +
         ". Did you open a file on tmpfs or a network mount?");
  }

// In comparison to O_DIRECT on Linux, we need to use fcntl(F_NOCACHE) for direct IO. See comment above.
#ifdef __APPLE__
  if (fcntl(fd, F_NOCACHE, 1) == -1) {
    const auto error = errno;
    close(fd);
    Fail("Error while setting F_NOCACHE on __APPLE__: " + strerror(error));
  }
#endif

  return fd;
}

void StorageRegion::write_page(PageID page_id, std::byte* data) {
  Assert(page_id.valid(), "PageID must be valid");
  const auto handle = _file_handles[static_cast<uint64_t>(page_id.size_type())];
  const auto byte_count = page_id.byte_count();
  const auto offset = handle.offset + byte_count * page_id.index();
  DebugAssertPageAlignment(data);
  if (pwrite(handle.fd, data, byte_count, offset) < 0) {
    const auto error = errno;
    Fail("Error while writing to StorageRegion: " + strerror(error));
  }
  _total_bytes_written.fetch_add(byte_count, std::memory_order_relaxed);
}

void StorageRegion::read_page(PageID page_id, std::byte* data) {
  Assert(page_id.valid(), "PageID must be valid");
  const auto handle = _file_handles[static_cast<uint64_t>(page_id.size_type())];
  const auto byte_count = page_id.byte_count();
  const auto offset = handle.offset + byte_count * page_id.index();
  DebugAssertPageAlignment(data);
  if (pread(handle.fd, data, byte_count, offset) < 0) {
    const auto error = errno;
    Fail("Error while reading from StorageRegion: " + strerror(error));
  }
  _total_bytes_read.fetch_add(byte_count, std::memory_order_relaxed);
}

size_t StorageRegion::memory_consumption() const {
  return sizeof(*this);
}

std::array<StorageRegion::FileHandle, PAGE_SIZE_TYPES_COUNT> StorageRegion::_open_file_handles_in_directory(
    const std::filesystem::path& path) {
  DebugAssert(std::filesystem::is_directory(path), "StorageRegion path must be a directory");
  auto array = std::array<StorageRegion::FileHandle, PAGE_SIZE_TYPES_COUNT>{};

  const auto now = std::chrono::system_clock::now();
  const auto timestamp = std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count();

  for (auto page_size_type_index = size_t{0}; page_size_type_index < PAGE_SIZE_TYPES_COUNT; ++page_size_type_index) {
    const auto file_name = path / ("hyrise-buffer-pool-" + std::to_string(timestamp) + "-type-" +
                                   std::to_string(page_size_type_index) + ".bin");
    array[page_size_type_index] = {_open_file_descriptor(file_name), file_name};
    // We initialize the file with 32 MiB.
    std::filesystem::resize_file(file_name, 1UL << 25);
  }

  return array;
}

std::array<StorageRegion::FileHandle, PAGE_SIZE_TYPES_COUNT> StorageRegion::_open_file_handles_block(
    const std::filesystem::path& path) {
  DebugAssert(std::filesystem::is_block_file(path), "StorageRegion path must be a directory");
  auto array = std::array<StorageRegion::FileHandle, PAGE_SIZE_TYPES_COUNT>{};

  const auto fd = _open_file_descriptor(path);

  // ioctl with the BLKGETSIZE64 allows us to get the size of the block device on Linux
  // It is not supported on Mac
  auto block_size = uint64_t{0};
#ifdef __APPLE__
  Fail("Block device not supported on Mac");
#elif __linux__
  if (ioctl(fd, BLKGETSIZE64, &block_size) < 0) {
    const auto error = errno;
    Fail("Failed to get size of block device: " + strerror(error));
  }
#endif

  // Divide block device into equal chunks and align to page boundary
  const auto bytes_per_size_type = ((block_size / PAGE_SIZE_TYPES_COUNT) / OS_PAGE_SIZE) * OS_PAGE_SIZE;
  for (auto page_size_type_index = uint64_t{0}; page_size_type_index < PAGE_SIZE_TYPES_COUNT; ++page_size_type_index) {
    const auto block_offset = page_size_type_index * bytes_per_size_type;
    array[page_size_type_index] = {fd, path, block_offset};
  }

  return array;
}

uint64_t StorageRegion::total_bytes_written() const {
  return _total_bytes_written.load(std::memory_order_relaxed);
}

uint64_t StorageRegion::total_bytes_read() const {
  return _total_bytes_read.load(std::memory_order_relaxed);
}
}  // namespace hyrise
