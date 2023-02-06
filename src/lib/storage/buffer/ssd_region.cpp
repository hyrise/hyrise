#include "ssd_region.hpp"
#include "page.hpp"

namespace hyrise {

static SSDRegion::DeviceType find_device_type_or_fail(const std::filesystem::path& file_name) {
  if (std::filesystem::is_regular_file(file_name)) {
    return SSDRegion::DeviceType::REGULAR_FILE;
  } else if (std::filesystem::is_block_file(file_name)) {
    return SSDRegion::DeviceType::REGULAR_FILE;
  } else {
    Fail("The backing file has to be either a regular file or a block device");
  }
}

SSDRegion::SSDRegion(const std::filesystem::path& file_name, const uint64_t initial_num_bytes)
    : _fd_stream(open_file_descriptor(file_name)),
      _backing_file(_fd_stream.get()),
      _backing_file_name(file_name),
      _device_type(find_device_type_or_fail(file_name)) {
  if (_device_type == DeviceType::REGULAR_FILE) {
    std::filesystem::resize_file(file_name, initial_num_bytes);
  }
}

SSDRegion::~SSDRegion() {
  if(_device_type == eviceType::REGULAR_FILE) {
std::filesystem::remove(_backing_file_name);  
  }
}

SSDRegion::DeviceType SSDRegion::get_device_type() const {
  return _device_type;
}

std::unique_ptr<boost::iostreams::stream_buffer<boost::iostreams::file_descriptor>> SSDRegion::open_file_descriptor(
    const std::filesystem::path& file_name) {
#ifdef __APPLE__
  int flags = O_RDWR | O_SYNC | O_CREAT;
#elif __linux__
  int flags = O_RDWR | O_CREAT;
#endif
  int fd = open(std::string(file_name).c_str(), flags, 0666);
  if (fd < 0) {
    Fail("Error while opening backing file at " + std::string(file_name) + ": "+ strerror(errno));
  }
  auto fpstream = std::make_unique<boost::iostreams::stream_buffer<boost::iostreams::file_descriptor>>(
      fd, boost::iostreams::close_handle);

// Set F_NOCACHE on OS X, which is equivalent to O_DIRECT on Linux
#ifdef __APPLE__
  int res = fcntl(fd, F_NOCACHE, 1);
  if (res == -1) {
    Fail("Error while setting F_NOCACHE on __APPLE__: " + strerror(errno));
  }
#endif

  // if (!std::filesystem::exists(file_name)) {
  //   Fail("Backing file does not exist");
  // }

  return fpstream;
}

void SSDRegion::write_page(const PageID page_id, Page& source) {
  const off_t page_pos = page_id * PAGE_SIZE;
  _backing_file.seekg(page_pos);
  // Using reinterpret_cast is necessary here. Even the C++ StdLib does it in their examples.
  _backing_file.write(reinterpret_cast<char*>(source.data.data()), PAGE_SIZE);
  _backing_file.sync();
}

void SSDRegion::read_page(const PageID page_id, Page& destination) {
  const off_t page_pos = page_id * PAGE_SIZE;
  _backing_file.seekg(page_pos);
  // Using reinterpret_cast is necessary here. Even the C++ StdLib does it in their examples.
  _backing_file.read(reinterpret_cast<char*>(destination.data.data()), PAGE_SIZE);
}

}  // namespace hyrise