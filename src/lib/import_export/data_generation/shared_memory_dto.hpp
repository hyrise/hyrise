#pragma once

#include <cstdint>

namespace hyrise {
#define SHARED_MEMORY_FIELD_SIZE (3 * 64)
#define SHARED_MEMORY_DATA_SLOTS 8'388'608 // 2 ** 23
#define SHARED_MEMORY_DATA_BUFFER_OFFSET 268'435'456 // start at one eighth of the buffer, location 2**28

enum RingBufferCellType { Noop = 42u, TableSchema = 1u, TableGenerationInfo = 2u, Data = 3u, TableCompleted = 4u };

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

template<uint32_t work_unit_size, uint32_t num_columns>
struct SharedMemoryDataCell {
  char data[work_unit_size][num_columns][SHARED_MEMORY_FIELD_SIZE];
};

template <uint32_t buffer_size, uint32_t work_unit_size, uint32_t num_columns>
struct DataBuffer {
  SharedMemoryDataCell<work_unit_size, num_columns> data[buffer_size];

  SharedMemoryDataCell<work_unit_size, num_columns>* get_addressed_by(RingBufferCell* cell) {
    return &data[cell->data_buffer_offset / work_unit_size / num_columns / SHARED_MEMORY_FIELD_SIZE];
  }
};
} // namespace hyrise
