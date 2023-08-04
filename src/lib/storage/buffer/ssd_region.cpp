#include "ssd_region.hpp"
#ifdef __linux__
#include <linux/fs.h>
#include <sys/ioctl.h>
#endif

#include <unistd.h>

namespace hyrise {

// TODO: Properly support block device

static SSDRegion::Mode find_mode_or_fail(const std::filesystem::path& file_name) {
  if (std::filesystem::is_directory(file_name)) {
    return SSDRegion::Mode::FILE_PER_SIZE_TYPE;
  } else if (std::filesystem::is_block_file(file_name)) {
    return SSDRegion::Mode::BLOCK;
  } else {
    Fail("The backing file has to be either a directory or a block device");
  }
}

SSDRegion::SSDRegion(const std::filesystem::path& path, std::shared_ptr<BufferManagerMetrics> metrics)
    : _mode(find_mode_or_fail(path)),
      _file_handles(_mode == Mode::FILE_PER_SIZE_TYPE ? open_file_handles_in_directory(path)
                                                      : open_file_handles_block(path)),

      _metrics(metrics) {}

SSDRegion::~SSDRegion() {
  for (auto& file_handle : _file_handles) {
    close(file_handle.fd);
    // TODO != 0) {
    //   const auto error = errno;
    //   Fail("Error while closing file descriptor " + std::to_string(file_handle.fd) + ": " + strerror(error));
    // }
    // TODO: Assert( == 0, "Error while closing file descriptor");
    if (_mode == Mode::FILE_PER_SIZE_TYPE) {
      std::filesystem::remove(file_handle.backing_file_name);
    }
  }
}

SSDRegion::Mode SSDRegion::get_mode() const {
  return _mode;
}

int SSDRegion::open_file_descriptor(const std::filesystem::path& file_name) {
  // TODO: https://github.com/google/leveldb/commit/296de8d5b8e4e57bd1e46c981114dfbe58a8c4fa
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

void SSDRegion::write_page(PageID page_id, std::byte* data) {
  const auto handle = _file_handles[page_id._size_type];
  const auto num_bytes = page_id.num_bytes();
  const size_t pos = handle.offset + num_bytes * page_id.index;
  DebugAssertPageAligned(data);
  const auto result = pwrite(handle.fd, data, num_bytes, pos);
  if (result < 0) {
    const auto error = errno;
    Fail("Error while writing to SSDRegion: " + strerror(error));
  }
  _metrics->total_bytes_copied_to_ssd.fetch_add(num_bytes, std::memory_order_relaxed);
  // TODO: Needs flush?
}

void SSDRegion::read_page(PageID page_id, std::byte* data) {
  const auto handle = _file_handles[page_id._size_type];
  const auto num_bytes = page_id.num_bytes();
  const auto pos = handle.offset + num_bytes * page_id.index;
  DebugAssertPageAligned(data);
  const auto result = pread(handle.fd, data, num_bytes, pos);
  if (result < 0) {
    const auto error = errno;
    Fail("Error while reading from SSDRegion: " + strerror(error));
  }
  _metrics->total_bytes_copied_from_ssd.fetch_add(num_bytes, std::memory_order_relaxed);
}

size_t SSDRegion::memory_consumption() const {
  return sizeof(*this);
}

std::array<SSDRegion::FileHandle, NUM_PAGE_SIZE_TYPES> SSDRegion::open_file_handles_in_directory(
    const std::filesystem::path& path) {
  DebugAssert(std::filesystem::is_directory(path), "SSDRegion path must be a directory");
  auto array = std::array<SSDRegion::FileHandle, NUM_PAGE_SIZE_TYPES>{};

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

std::array<SSDRegion::FileHandle, NUM_PAGE_SIZE_TYPES> SSDRegion::open_file_handles_block(
    const std::filesystem::path& path) {
  DebugAssert(std::filesystem::is_block_file(path), "SSDRegion path must be a directory");
  auto array = std::array<SSDRegion::FileHandle, NUM_PAGE_SIZE_TYPES>{};

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

void swap(SSDRegion& first, SSDRegion& second) noexcept {
  using std::swap;
  swap(first._file_handles, second._file_handles);
  swap(first._mode, second._mode);
  swap(first._metrics, second._metrics);
}

}  // namespace hyrise