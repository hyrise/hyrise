#pragma once

#include <fcntl.h>
#include <boost/filesystem/fstream.hpp>
#include <boost/iostreams/device/file_descriptor.hpp>
#include <boost/iostreams/stream.hpp>
#include <filesystem>
#include "storage/buffer/page.hpp"
#include "storage/buffer/types.hpp"

namespace hyrise {

class SSDRegion  {
 public:
  enum class DeviceType { BLOCK, REGULAR_FILE };

  SSDRegion(const std::filesystem::path& file_name, const uint64_t initial_num_bytes = 1UL << 32);
  ~SSDRegion();

  void write_page(const PageID page_id, Page32KiB& destination);
  void read_page(const PageID page_id, Page32KiB& source);

  DeviceType get_device_type() const;

 private:
  const int _fd;
  const std::filesystem::path _backing_file_name;
  const DeviceType _device_type;

  static int open_file_descriptor(const std::filesystem::path& file_name);
};
}  // namespace hyrise