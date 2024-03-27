#pragma once

#include <fcntl.h>
#include <filesystem>
#include <mutex>
#include <unordered_map>
#include "page_id.hpp"
#include "types.hpp"

namespace hyrise {

/**
 * StorageRegion manages read and write access of pages to an SSD. StorageRegion can either use a block device directly by assigning equal-sized regions
 * to each page size type or use a directory with one file for each page size type. Files are opened using direct IO to bypass the OS page cache. On Linux, we
 * use the O_DIRECT when opening the file. On OS X, we use the F_NOCACHE flag after opening the file. This is equivalent to O_DIRECT on Linux.
 * This requires read and write buffers to be aligned to the page boundary (e.g. 4KB). Destructing the PersistenceManager closes all open file descriptors and 
 * deletes all files in the directory. Block devices are not cleared.
 * 
 * Writing and reading concurrently to different pages is thread-safe. Accessing the same page should be guarded with concurrency primitives accordingly. 
 * We usually use the exclusive latching defined by the Frame class. Currently, we only support synchronous IO, which means that the calling thread is 
 * blocked until the IO operation has finished. Employing aio, libaio or io_uring is considered future work, but may lead to significant performance improvements.
 */
class StorageRegion final : public Noncopyable {
 public:
  // The mode can be either a block device or a directory with one file for each page size type.
  enum class Mode { BLOCK, FILE_PER_PAGE_SIZE };

  // Creates a new StorageRegion. The path can either be a directory or a block device.
  // If the path is a directory, a file is created for each page size type.
  StorageRegion(const std::filesystem::path& path);

  // Destroy the StorageRegion and close all open file descriptors.
  // This also deletes all files in the directory. Block devices are not cleared.
  ~StorageRegion();

  // Writes the given data to the page with the given page id.
  // The buffer address must be aligned to the page size boundary.
  void write_page(const PageID page_id, std::byte* data);

  // Reads the data of the page with the given page id into the given buffer.
  // The buffer address must be aligned to the page size boundary.
  void read_page(const PageID page_id, std::byte* data);

  // Returns the mode of the StorageRegion.
  Mode mode() const;

  // Returns the consumed memory in bytes for debugging purposes.
  size_t memory_consumption() const;

  // Returns the total number of bytes written to the disk.
  uint64_t total_bytes_written() const;

  // Returns the total number of bytes read from the disk.
  uint64_t total_bytes_read() const;

 private:
  // Each page size type has its own file handle. The file handle contains
  // the file descriptor and the backing file name.
  struct FileHandle {
    // File descriptor of backing file
    int fd;

    // Filename of backing file
    std::filesystem::path backing_file_name;

    // Use for block devices and single file (not implemented yet)
    size_t offset = 0;
  };

  const Mode _mode;
  const std::array<FileHandle, PAGE_SIZE_TYPES_COUNT> _file_handles;

  // Internal metrics
  std::atomic_uint64_t _total_bytes_written = 0;
  std::atomic_uint64_t _total_bytes_read = 0;

  // Open file haswndles for the given path. If the path is a directory,
  // a file is created for each page size type.
  std::array<FileHandle, PAGE_SIZE_TYPES_COUNT> _open_file_handles_in_directory(const std::filesystem::path& path);

  // Open file handles for the given path. If the path is a block device,
  // the device is divided into equal-sized regions for each page size type.
  std::array<FileHandle, PAGE_SIZE_TYPES_COUNT> _open_file_handles_block(const std::filesystem::path& path);

  static int _open_file_descriptor(const std::filesystem::path& file_name);
};
}  // namespace hyrise
