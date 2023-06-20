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

  SSDRegion(const std::filesystem::path& path);
  ~SSDRegion();

  void write_page(const PageID page_id, std::byte* data);
  void read_page(const PageID page_id, std::byte* data);

  DeviceType get_device_type() const;

  size_t memory_consumption() const;

  SSDRegion& operator=(SSDRegion&& other) noexcept;

 private:
  struct FileHandle {
    int fd;
    std::filesystem::path backing_file_name;
  };

  std::array<FileHandle, NUM_PAGE_SIZE_TYPES> _file_handles;
  DeviceType _device_type;

  std::array<FileHandle, NUM_PAGE_SIZE_TYPES> open_file_handles(const std::filesystem::path& path);
  static int open_file_descriptor(const std::filesystem::path& file_name);
};
}  // namespace hyrise