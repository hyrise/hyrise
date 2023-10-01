#pragma once

#include <fcntl.h>
#include <filesystem>
#include <mutex>
#include <unordered_map>
#include "noncopyable.hpp"
#include "storage/buffer/helper.hpp"
#include "storage/buffer/metrics.hpp"

namespace hyrise {

/**
 * SSDRegion manages read and write access of pages to an SSD. SSDRegion can either use a block device directly by assigning equal-sized regions
 * to each page size type or use a directory with one file for each page size type. Files are opened using direct IO (O_DIRECT) to skip the OS page cache. 
 * This requires read and write buffers to be aligned to the page boundary (e.g. 4KB). Writing and reading concurrently to different pages is thread-safe.
 * We currently only support synchronous IO, which means that the calling thread is blocked until the IO operation is finished. 
 * Employing aio, libaio or io_uring is considered future work, but may lead to significant performance improvements.
 */
class SSDRegion final : public Noncopyable {
 public:
  enum class Mode { BLOCK, FILE_PER_SIZE_TYPE };

  /**
   * Creates a new SSDRegion. The path can either be a directory or a block device. If the path is a directory, a file is created for each page size type.
  */
  SSDRegion(const std::filesystem::path& path);

  ~SSDRegion();

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

  std::atomic_uint64_t total_bytes_copied_to_ssd = 0;
  std::atomic_uint64_t total_bytes_copied_from_ssd = 0;

  std::array<FileHandle, NUM_PAGE_SIZE_TYPES> open_file_handles_in_directory(const std::filesystem::path& path);
  std::array<FileHandle, NUM_PAGE_SIZE_TYPES> open_file_handles_block(const std::filesystem::path& path);

  static int open_file_descriptor(const std::filesystem::path& file_name);
};
}  // namespace hyrise