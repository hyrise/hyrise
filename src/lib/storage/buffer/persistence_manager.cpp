#include "persistence_manager.hpp"
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

static PersistenceManager::Mode find_mode_or_fail(const std::filesystem::path& file_name) {
  if (std::filesystem::is_directory(file_name)) {
    return PersistenceManager::Mode::FILE_PER_SIZE_TYPE;
  } else if (std::filesystem::is_block_file(file_name)) {
    return PersistenceManager::Mode::BLOCK;
  } else {
    Fail("The backing file has to be either a directory or a block device: " + file_name.string());
  }
}

PersistenceManager::PersistenceManager(const std::filesystem::path& path)
    : _mode(find_mode_or_fail(path)),
      _file_handles(_mode == Mode::FILE_PER_SIZE_TYPE ? open_file_handles_in_directory(path)
                                                      : open_file_handles_block(path))

{}

PersistenceManager::~PersistenceManager() {
  for (auto& file_handle : _file_handles) {
    // We do not handle errors of the close syscall, since we are already in the destructor
    close(file_handle.fd);
    if (_mode == Mode::FILE_PER_SIZE_TYPE) {
      std::filesystem::remove(file_handle.backing_file_name);
    }
  }
}

PersistenceManager::Mode PersistenceManager::mode() const {
  return _mode;
}

int PersistenceManager::open_file_descriptor(const std::filesystem::path& file_name) {
  // Partially taken from
  // DuckDB: https://github.com/duckdb/duckdb/blob/60ed227816669be497fa4ba53e593d3899479c43/src/common/local_file_system.cpp
  // LevelDB: https://github.com/google/leveldb/commit/296de8d5b8e4e57bd1e46c981114dfbe58a8c4fa
#ifdef __APPLE__
  int flags = O_RDWR | O_CREAT | O_DSYNC;
#elif __linux__
  int flags = O_RDWR | O_CREAT | O_DIRECT | O_DSYNC;
#endif
  int fd = open(file_name.string().c_str(), flags, 0666);
  if (fd < 0) {
    const auto error = errno;
    close(fd);
    Fail("Error while opening backing file at " + file_name.string().c_str() + ": " + strerror(error) +
         ". Did you open a file on tmpfs or a network mount?");
  }

// Set F_NOCACHE on OS X, which is equivalent to O_DIRECT on Linux
#ifdef __APPLE__
  int res = fcntl(fd, F_NOCACHE, 1);
  if (res == -1) {
    const auto error = errno;
    close(fd);
    Fail("Error while setting F_NOCACHE on __APPLE__: " + strerror(error));
  }
#endif

  return fd;
}

void PersistenceManager::write_page(PageID page_id, std::byte* data) {
  Assert(page_id.valid(), "PageID must be valid");
  const auto handle = _file_handles[page_id._size_type];
  const auto num_bytes = page_id.num_bytes();
  const size_t pos = handle.offset + num_bytes * page_id.index;
  DebugAssertPageAlignment(data);
  if (pwrite(handle.fd, data, num_bytes, pos) < 0) {
    const auto error = errno;
    Fail("Error while writing to PersistenceManager: " + strerror(error));
  }
  _total_bytes_written.fetch_add(num_bytes, std::memory_order_relaxed);
}

void PersistenceManager::read_page(PageID page_id, std::byte* data) {
  Assert(page_id.valid(), "PageID must be valid");
  const auto handle = _file_handles[page_id._size_type];
  const auto num_bytes = page_id.num_bytes();
  const auto pos = handle.offset + num_bytes * page_id.index;
  DebugAssertPageAlignment(data);
  if (pread(handle.fd, data, num_bytes, pos) < 0) {
    const auto error = errno;
    Fail("Error while reading from PersistenceManager: " + strerror(error));
  }
  _total_bytes_read.fetch_add(num_bytes, std::memory_order_relaxed);
}

size_t PersistenceManager::memory_consumption() const {
  return sizeof(*this);
}

std::array<PersistenceManager::FileHandle, NUM_PAGE_SIZE_TYPES> PersistenceManager::open_file_handles_in_directory(
    const std::filesystem::path& path) {
  DebugAssert(std::filesystem::is_directory(path), "PersistenceManager path must be a directory");
  auto array = std::array<PersistenceManager::FileHandle, NUM_PAGE_SIZE_TYPES>{};

  const auto now = std::chrono::system_clock::now();
  const auto timestamp = std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count();

  for (auto i = size_t{0}; i < NUM_PAGE_SIZE_TYPES; ++i) {
    const auto file_name =
        path / ("hyrise-buffer-pool-" + std::to_string(timestamp) + "-type-" + std::to_string(i) + ".bin");
    array[i] = {open_file_descriptor(file_name), file_name};
    std::filesystem::resize_file(file_name, 1UL << 25);
  }

  return array;
}

std::array<PersistenceManager::FileHandle, NUM_PAGE_SIZE_TYPES> PersistenceManager::open_file_handles_block(
    const std::filesystem::path& path) {
  DebugAssert(std::filesystem::is_block_file(path), "PersistenceManager path must be a directory");
  auto array = std::array<PersistenceManager::FileHandle, NUM_PAGE_SIZE_TYPES>{};

  const auto fd = open_file_descriptor(path);

  auto block_size = size_t{0};

#ifdef __APPLE__
  Fail("Block device not supported on Mac");
#elif __linux__
  if (ioctl(fd, BLKGETSIZE64, &block_size) < 0) {
    const auto error = errno;
    Fail("Failed to get size of block device: " + strerror(error));
  }
#endif

  // Divide block device into equal chunks and align to page boundary
  const auto bytes_per_size_type = ((block_size / NUM_PAGE_SIZE_TYPES) / OS_PAGE_SIZE) * OS_PAGE_SIZE;
  for (auto i = size_t{0}; i < NUM_PAGE_SIZE_TYPES; ++i) {
    const auto block_offset = i * bytes_per_size_type;
    array[i] = {fd, path, block_offset};
  }

  return array;
}

void swap(PersistenceManager& first, PersistenceManager& second) noexcept {
  std::swap(first._file_handles, second._file_handles);
  std::swap(first._mode, second._mode);

  // The exchange is not atomic
  const auto copied_to_ssd_tmp = first._total_bytes_written.load();
  first._total_bytes_written = second._total_bytes_written.load();
  second._total_bytes_written = copied_to_ssd_tmp;

  const auto copied_from_ssd_tmp = first._total_bytes_read.load();
  first._total_bytes_read = second._total_bytes_read.load();
  second._total_bytes_read = copied_from_ssd_tmp;
}

uint64_t PersistenceManager::total_bytes_written() const {
  return _total_bytes_written.load(std::memory_order_relaxed);
}

uint64_t PersistenceManager::total_bytes_read() const {
  return _total_bytes_read.load(std::memory_order_relaxed);
}
}  // namespace hyrise