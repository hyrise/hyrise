#pragma once

#include <fcntl.h>
#include <boost/filesystem/fstream.hpp>
#include <boost/iostreams/device/file_descriptor.hpp>
#include <boost/iostreams/stream.hpp>
#include <filesystem>
#include <mutex>
#include <unordered_map>
#include "boost/noncopyable.hpp"
#include "storage/buffer/page.hpp"
#include "storage/buffer/types.hpp"

namespace hyrise {

// TODO: Use extra object called SSDHandle to avoid page lockups
class SSDRegion : private boost::noncopyable {
 public:
  enum class DeviceType { BLOCK, REGULAR_FILE };

  SSDRegion(const std::filesystem::path& file_name, const uint64_t initial_num_bytes = 1UL << 25);
  ~SSDRegion();

  void write_page(const PageID page_id, const PageSizeType size_type, const std::byte* source);
  void read_page(const PageID page_id, const PageSizeType size_type, std::byte* destination);

  void register_page(const PageID page_id, const PageSizeType size_type);
  std::optional<PageSizeType> get_size_type(const PageID page_id);

  DeviceType get_device_type() const;

  std::filesystem::path get_file_name();

 private:
  const int _fd;
  const std::filesystem::path _backing_file_name;
  const DeviceType _device_type;

  // Last position in file. Used when writing a new page
  size_t _end_position;

  std::vector<std::pair<std::size_t, PageSizeType>> _page_directory;
  std::mutex _page_directory_mutex;

  static int open_file_descriptor(const std::filesystem::path& file_name);
};
}  // namespace hyrise