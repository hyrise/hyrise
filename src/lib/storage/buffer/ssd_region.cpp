#include "ssd_region.hpp"
#include "page.hpp"

namespace hyrise {
SSDRegion::SSDRegion(const std::string_view file_name)
    : _fd_stream(open_file_descriptor(file_name)), _backing_file(_fd_stream.get()), _backing_file_name(file_name) {}

SSDRegion::~SSDRegion() {
  if (!std::filesystem::remove(_backing_file_name)) {
    Fail("Failed to remove backing file: " + std::string(_backing_file_name));
  }
}

// TODO: Preallocate size of SSDRegion to reduce read e.g with fallocate on Linux

std::unique_ptr<boost::iostreams::stream_buffer<boost::iostreams::file_descriptor>> SSDRegion::open_file_descriptor(
    const std::string_view file_name) {
#ifdef __APPLE__
  int flags = O_RDWR | O_SYNC | O_CREAT;
#elif __linux__
  int flags = O_RDWR | O_DIRECT | O_SYNC | O_CREAT;
#endif
  int fd = open(std::string(file_name).c_str(), flags, 0666);
  if (fd < 0) {
    Fail("Error while opening backing file: " + strerror(errno));
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

  return fpstream;
}

// TODO: Remove backing file

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