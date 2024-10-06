#pragma once

#include <cstdint>
#include <memory>
#include <semaphore.h>
#include <mutex>

namespace hyrise {
#define SHARED_MEMORY_FIELD_SIZE (3 * 64)

enum RingBufferCellType { Noop = 42u, TableInfo = 1u, Data = 2u, TableCompleted = 3u };

struct RingBufferCell {
  RingBufferCellType cell_type;  // TODO: using uints might break
  uint32_t data_buffer_offset;
  uint32_t table_id;
  uint32_t pad;
  int64_t sorting_id;
  int64_t table_num_rows;
  uint32_t padding[8];
};

template <uint32_t buffer_size>
struct RingBuffer {
  RingBufferCell cells[buffer_size];
};

template <uint32_t buffer_size>
class MultiProcessRingBuffer {
 public:
  explicit MultiProcessRingBuffer(int shared_memory_fd, uint32_t workunit_size, uint32_t num_columns,
                                  const char* data_available_sem_path, const char* data_written_sem_path);
  ~MultiProcessRingBuffer();

  RingBufferCell* prepare_retrieval();
  void retrieval_finished();

  RingBufferCell* prepare_writing();
  void writing_finished();

 protected:
  void _initialize();

  RingBuffer<buffer_size>* _ring_buffer;

  uint32_t _workunit_size;
  uint32_t _num_columns;

  const char* _data_available_sem_path;
  sem_t* _data_available_semaphore;
  uint32_t _current_read_index;
  std::mutex _read_access_semaphore;

  const char* _data_written_sem_path;
  sem_t* _data_written_semaphore;
  uint32_t _current_write_index;
  std::mutex _write_access_semaphore;
};

extern template class MultiProcessRingBuffer<4096u>;
} // namespace hyrise
