#pragma once

#include <cstdint>
#include <iostream>
#include "multi_process_ring_buffer.hpp"
#include "pdgf_table_builder.hpp"
#include "shared_memory_dto.hpp"
#include "storage/table.hpp"

namespace hyrise {
template <uint32_t work_unit_size, uint32_t num_columns>
class SharedMemoryReader : Noncopyable {
 public:
  static const uint32_t buffer_size = SHARED_MEMORY_DATA_SLOTS / work_unit_size / num_columns;

  explicit SharedMemoryReader(ChunkOffset hyrise_table_chunk_size, const char* shared_memory_name, const char* data_ready_sem, const char* buffer_free_sem);
  ~SharedMemoryReader();

  bool has_next_table();
  std::unique_ptr<PDGFTableBuilder<work_unit_size, num_columns>> read_next_table();

 protected:
  void _return_data_slot(uint32_t buffer_offset);

  ChunkOffset _hyrise_table_chunk_size;
  const char* _shared_memory_file_name;
  int _shm_fd;
  DataBuffer<buffer_size, work_unit_size, num_columns>* _data_buffer;
  std::shared_ptr<MultiProcessRingBuffer<buffer_size>> _ring_buffer;

  int32_t _num_tables_to_read = INT32_MAX;
  int32_t _num_read_tables = 0;
};

template class SharedMemoryReader<128u, 16u>;
} // namespace hyrise
