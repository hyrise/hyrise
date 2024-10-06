#pragma once

#include <cstdint>
#include <iostream>
#include "import_export/data_generation/multi_process_ring_buffer.hpp"

namespace hyrise {
#define SHARED_MEMORY_DATA_SLOTS 8'388'608 // 2 ** 23
#define SHARED_MEMORY_DATA_BUFFER_OFFSET 268'435'456 // start at one eighth of the buffer, location 2**28

template<uint32_t work_unit_size, uint32_t num_columns>
struct DataCell {
  char data[work_unit_size][num_columns][SHARED_MEMORY_FIELD_SIZE];
};

template <uint32_t buffer_size, uint32_t work_unit_size, uint32_t num_columns>
struct DataBuffer {
  DataCell<work_unit_size, num_columns> data[buffer_size];
};

template <uint32_t work_unit_size, uint32_t num_columns>
class SharedMemoryReader {
 public:
  static const uint32_t buffer_size = SHARED_MEMORY_DATA_SLOTS / work_unit_size / num_columns;

  explicit SharedMemoryReader(const char* shared_memory_name, const char* data_ready_sem, const char* buffer_free_sem);
  ~SharedMemoryReader();

  void read_data();

 protected:
  bool _read_next_cell();
  void _return_data_slot(uint32_t buffer_offset);

  void _read_table_info_cell(RingBufferCell* cell);
  void _read_data_cell(RingBufferCell* cell);
  void _read_table_complete_cell(RingBufferCell* cell);


  DataCell<work_unit_size, num_columns>* _addressed_data_by_cell(RingBufferCell* cell);

  const char* _shared_memory_file_name;
  int _shm_fd;
  DataBuffer<buffer_size, work_unit_size, num_columns>* _data_buffer;
  std::shared_ptr<MultiProcessRingBuffer<buffer_size>> _ring_buffer;

  int32_t _num_tables_to_read = INT32_MAX;
  int32_t _num_read_tables = 0;
};

template class SharedMemoryReader<128u, 16u>;
} // namespace hyrise
