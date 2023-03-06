#include "ssd_region.hpp"
#include <unistd.h>
#include "page.hpp"

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

SSDRegion::SSDRegion(const std::filesystem::path& file_name, const uint64_t initial_num_bytes)
    : _fd(open_file_descriptor(file_name)),
      _backing_file_name(file_name),
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
#ifdef __APPLE__
  int flags = O_RDWR | O_CREAT;
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
    close(fd);
    Fail("Error while setting F_NOCACHE on __APPLE__: " + strerror(errno));
  }
#endif

  return fd;
}

// TODO: Write test for reinterpret, maybe use custom operaor

void SSDRegion::write_page(const PageID page_id, const PageSizeType size_type, const std::byte* source) {
  const off_t page_pos = page_id * bytes_for_size_type(MIN_PAGE_SIZE_TYPE);
  // Using reinterpret_cast is necessary here. Even the C++ StdLib does it in their examples.
  // TODO: DebugAssert(static_cast<const std::uintptr_t>(source) % 512 == 0, "Source is not properly aligned to 512");
  const auto result = pwrite(_fd, source, bytes_for_size_type(size_type), page_pos);
  if (result < 0) {
    const auto error = errno;
    Fail("Error while writing to SSDRegion: " + strerror(error));
  }
}

void SSDRegion::read_page(const PageID page_id, const PageSizeType size_type, std::byte* destination) {
  const off_t page_pos = page_id * bytes_for_size_type(MIN_PAGE_SIZE_TYPE);
  // Using reinterpret_cast is necessary here. Even the C++ StdLib does it in their examples.
  const auto result = pread(_fd, destination, bytes_for_size_type(size_type), page_pos);
  if (result < 0) {
    const auto error = errno;
    Fail("Error while reading from SSDRegion: " + strerror(error));
  }
}

std::filesystem::path SSDRegion::get_file_name() {
  return _backing_file_name;
}
}  // namespace hyrise