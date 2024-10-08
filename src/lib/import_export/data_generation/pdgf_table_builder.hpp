#pragma once

#include <cstdint>

#include "shared_memory_dto.hpp"
#include "storage/table.hpp"

namespace hyrise {

template <uint32_t work_unit_size, uint32_t num_columns>
class PDGFTableBuilder : Noncopyable {
 public:
  explicit PDGFTableBuilder(uint32_t table_id, uint32_t hyrise_table_chunk_size, int64_t table_num_rows);

  bool expects_more_data();
  std::shared_ptr<Table> construct_table();

  void read_schema(SharedMemoryDataCell<work_unit_size, num_columns>* schema_cell);

  void read_generation_info(SharedMemoryDataCell<work_unit_size, num_columns>* info_cell);

  void read_data(uint32_t table_id, int64_t sorting_id, SharedMemoryDataCell<work_unit_size, num_columns>* data_cell);

 protected:
  uint32_t _table_id;
  uint32_t _hyrise_table_chunk_size;
  int64_t _table_num_rows;
  int64_t _received_rows = 0;
};

template class PDGFTableBuilder<128u, 16u>;
} // namespace hyrise