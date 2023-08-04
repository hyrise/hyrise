#pragma once

#include <fcntl.h>
#include <filesystem>
#include <mutex>
#include <unordered_map>
#include "noncopyable.hpp"
#include "storage/buffer/metrics.hpp"
#include "storage/buffer/helper.hpp"

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
  enum class Mode { BLOCK, FILE_PER_SIZE_TYPE };

  SSDRegion(const std::filesystem::path& path, std::shared_ptr<BufferManagerMetrics> metrics);
  ~SSDRegion();

  SSDRegion(SSDRegion&& other) noexcept = default;
  SSDRegion& operator=(SSDRegion&& other) noexcept = default;

  void write_page(const PageID page_id, std::byte* data);
  void read_page(const PageID page_id, std::byte* data);

  Mode get_mode() const;

  size_t memory_consumption() const;

  friend void swap(SSDRegion& first, SSDRegion& second) noexcept;

 private:
  struct FileHandle {
    int fd;
    std::filesystem::path backing_file_name;
    size_t offset = 0;  // Use for block devices and single files
  };

  Mode _mode;
  std::array<FileHandle, NUM_PAGE_SIZE_TYPES> _file_handles;
  std::shared_ptr<BufferManagerMetrics> _metrics;

  std::array<FileHandle, NUM_PAGE_SIZE_TYPES> open_file_handles_in_directory(const std::filesystem::path& path);
  std::array<FileHandle, NUM_PAGE_SIZE_TYPES> open_file_handles_block(const std::filesystem::path& path);

  static int open_file_descriptor(const std::filesystem::path& file_name);
};
}  // namespace hyrise