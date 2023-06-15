#pragma once

#include <fcntl.h>
#include <filesystem>
#include <mutex>
#include <unordered_map>
#include "noncopyable.hpp"
#include "storage/buffer/types.hpp"

namespace hyrise {

// TODO: Use extra object called SSDHandle to avoid page directory lockups
// TODO: Use flag for O_DIRECT
/**
  * TODO
 * @brief Page wraps binary data to be written or read. It's aligned to 512 bytes in order to work with the O_DIRECT flag and SSDs. 
 * O_DIRECT is often used in databases when they implement their own caching/buffer management like in our case. The page size should also be a multiple of the OS' page size.
 */
class SSDRegion : public Noncopyable {
 public:
  enum class DeviceType { BLOCK, REGULAR_FILE };

  SSDRegion(const std::filesystem::path& file_name, const size_t max_bytes_per_size_type,
            const uint64_t initial_num_bytes = 1UL << 25);
  ~SSDRegion();

  void write_page(const PageID page_id, std::byte* data);
  void read_page(const PageID page_id, std::byte* data);

  DeviceType get_device_type() const;

  std::filesystem::path get_file_name();

  size_t memory_consumption() const;

  SSDRegion& operator=(SSDRegion&& other) noexcept;

 private:
  int _fd;
  size_t _max_bytes_per_size_type;
  std::filesystem::path _backing_file_name;
  DeviceType _device_type;

  static int open_file_descriptor(const std::filesystem::path& file_name);
};
}  // namespace hyrise