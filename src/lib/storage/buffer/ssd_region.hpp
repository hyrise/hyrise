#pragma once

#include <fcntl.h>
#include <boost/filesystem/fstream.hpp>
#include <boost/iostreams/device/file_descriptor.hpp>
#include <boost/iostreams/stream.hpp>
#include "storage/buffer/page.hpp"
#include "types.hpp"

namespace hyrise {

class SSDRegion : private Noncopyable {
 public:
  SSDRegion(const std::string_view file_name);
  ~SSDRegion();

  void write_page(const PageID page_id, Page& destination);
  void read_page(const PageID page_id, Page& source);

 private:
  std::unique_ptr<boost::iostreams::stream_buffer<boost::iostreams::file_descriptor>> _fd_stream;
  std::iostream _backing_file;
  const std::string_view _backing_file_name;

  static std::unique_ptr<boost::iostreams::stream_buffer<boost::iostreams::file_descriptor>> open_file_descriptor(
      const std::string_view file_name);
};
}  // namespace hyrise