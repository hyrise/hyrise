#include "ssd_region.hpp"
#include <unistd.h>

namespace hyrise {

static SSDRegion::DeviceType find_device_type_or_fail(const std::filesystem::path& file_name) {
  if (std::filesystem::is_regular_file(file_name)) {
    return SSDRegion::DeviceType::REGULAR_FILE;
  } else if (std::filesystem::is_block_file(file_name)) {
    return SSDRegion::DeviceType::BLOCK;
  } else {
    Fail("The backing file has to be either a regular file or a block device");
  }
}

SSDRegion::SSDRegion(const std::filesystem::path& file_name, const size_t max_bytes_per_size_type,
                     const uint64_t initial_num_bytes)
    : _fd(open_file_descriptor(file_name)),
      _backing_file_name(file_name),
      _max_bytes_per_size_type(max_bytes_per_size_type),
      _device_type(find_device_type_or_fail(file_name)) {
  if (_device_type == DeviceType::REGULAR_FILE) {
    std::filesystem::resize_file(file_name, initial_num_bytes);
  }
}

SSDRegion::~SSDRegion() {
  Assert(close(_fd) == 0, "Error while closing file descriptor");
  if (_device_type == DeviceType::REGULAR_FILE) {
    std::filesystem::remove(_backing_file_name);
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
  DebugAssertPageAligned(data);
  // auto pos = _max_bytes_per_size_type * TODO
  const auto result = pwrite(_fd, data, num_bytes, 0);
  if (result < 0) {
    const auto error = errno;
    Fail("Error while writing to SSDRegion: " + strerror(error));
  }
  // TODO Needs flush?
}

void SSDRegion::read_page(PageID page_id, std::byte* data) {
  const auto num_bytes = bytes_for_size_type(page_id.size_type());
  DebugAssertPageAligned(data);
  // Using reinterpret_cast is necessary here. Even the C++ StdLib does it in their examples.
  const auto result = pread(_fd, data, num_bytes, 0);
  if (result < 0) {
    const auto error = errno;
    Fail("Error while reading from SSDRegion: " + strerror(error));
  }
}

size_t SSDRegion::memory_consumption() const {
  return 0;  // TODO
}

SSDRegion& SSDRegion::operator=(SSDRegion&& other) noexcept {
  if (&other != this) {
    _fd = std::move(other._fd);
    _max_bytes_per_size_type = other._max_bytes_per_size_type;
    _backing_file_name = std::move(other._backing_file_name);
    _device_type = other._device_type;
  }
  return *this;
}

std::filesystem::path SSDRegion::get_file_name() {
  return _backing_file_name;
}
}  // namespace hyrise