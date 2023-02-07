#include <vector>

namespace hyrise {
struct BufferManagerMetrics {
  // Tracks all allocation that are happening on the buffer manager through the BufferPoolAllocator
  std::vector<std::size_t> allocations_in_bytes{};

  // Tracks the number of hits in the page_table
  std::size_t page_table_hits = 0;

  // Tracks the number of hits in the page_table
  std::size_t page_table_misses = 0;

  // Tracks the number of frames in the volatile region
  std::size_t num_frames = 0;

  // Tracks the number of bytes written to SSD
  std::size_t bytes_written = 0;

  // Tracks the number of bytes read from SSD
  std::size_t bytes_read = 0;
};
}  // namespace hyrise