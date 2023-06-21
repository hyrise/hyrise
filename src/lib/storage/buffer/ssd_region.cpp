#include "ssd_region.hpp"
#include <unistd.h>

namespace hyrise {

// TODO: Properly support block device

static SSDRegion::DeviceType find_device_type_or_fail(const std::filesystem::path& file_name) {
  if (std::filesystem::is_regular_file(file_name) || std::filesystem::is_directory(file_name)) {
    return SSDRegion::DeviceType::REGULAR_FILE;
  } else if (std::filesystem::is_block_file(file_name)) {
    return SSDRegion::DeviceType::BLOCK;
  } else {
    Fail("The backing file has to be either a regular file or a block device");
  }
}

SSDRegion::SSDRegion(const std::filesystem::path& path, std::shared_ptr<BufferManagerMetrics> metrics)
    : _file_handles(open_file_handles(path)), _device_type(find_device_type_or_fail(path)), _metrics(metrics) {}

SSDRegion::~SSDRegion() {
  for (auto& file_handle : _file_handles) {
    // TODO: Assert(close(file_handle.fd) == 0, "Error while closing file descriptor");
    if (_device_type == DeviceType::REGULAR_FILE) {
      std::filesystem::remove(file_handle.backing_file_name);
    }
  }
}

SSDRegion::DeviceType SSDRegion::get_device_type() const {
  return _device_type;
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
  const auto num_bytes = bytes_for_size_type(page_id.size_type());
  const auto pos = num_bytes * page_id.index;
  DebugAssertPageAligned(data);
  const auto result = pwrite(_file_handles[page_id._size_type].fd, data, num_bytes, pos);
  if (result < 0) {
    const auto error = errno;
    Fail("Error while writing to SSDRegion: " + strerror(error));
  }
  _metrics->total_bytes_copied_to_ssd.fetch_add(num_bytes, std::memory_order_relaxed);
  // TODO: Needs flush?
}

void SSDRegion::read_page(PageID page_id, std::byte* data) {
  const auto num_bytes = bytes_for_size_type(page_id.size_type());
  const auto pos = num_bytes * page_id.index;
  DebugAssertPageAligned(data);
  const auto result = pread(_file_handles[page_id._size_type].fd, data, num_bytes, pos);
  if (result < 0) {
    const auto error = errno;
    Fail("Error while reading from SSDRegion: " + strerror(error));
  }
  _metrics->total_bytes_copied_from_ssd.fetch_add(num_bytes, std::memory_order_relaxed);
}

size_t SSDRegion::memory_consumption() const {
  return 0;  // TODO
}

std::array<SSDRegion::FileHandle, NUM_PAGE_SIZE_TYPES> SSDRegion::open_file_handles(const std::filesystem::path& path) {
  DebugAssert(std::filesystem::is_directory(path), "SSDRegion path must be a directory");
  auto array = std::array<SSDRegion::FileHandle, NUM_PAGE_SIZE_TYPES>{};

  const auto now = std::chrono::system_clock::now();
  const auto timestamp = std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count();

  for (auto i = 0; i < NUM_PAGE_SIZE_TYPES; ++i) {
    const auto file_name =
        path / ("hyrise-buffer-pool-" + std::to_string(timestamp) + "-type-" + std::to_string(i) + ".bin");
    array[i] = {open_file_descriptor(file_name), file_name};
    if (_device_type == DeviceType::REGULAR_FILE) {
      std::filesystem::resize_file(file_name, 1UL << 25);
    }
  }

  return array;
}

void swap(SSDRegion& first, SSDRegion& second) noexcept {
  using std::swap;
  swap(first._file_handles, second._file_handles);
  swap(first._device_type, second._device_type);
  swap(first._metrics, second._metrics);
}

}  // namespace hyrise