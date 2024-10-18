#pragma once

#include <cstdint>
#include <string>
#include <memory>
#include <vector>
#include <array>

#include "shared_memory_dto.hpp"
#include "pdgf_column.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace hyrise {

template <uint32_t work_unit_size, uint32_t num_columns>
class PDGFTableBuilder : Noncopyable {
 public:
  explicit PDGFTableBuilder(uint32_t table_id, ChunkOffset hyrise_table_chunk_size);

  bool expects_more_data() const;
  std::string table_name() const;
  std::shared_ptr<Table> build_table();

  void read_schema(SharedMemoryDataCell<work_unit_size, num_columns>* schema_cell);
  void read_generation_info(SharedMemoryDataCell<work_unit_size, num_columns>* info_cell);
  void read_data(uint32_t table_id, int64_t sorting_id, SharedMemoryDataCell<work_unit_size, num_columns>* data_cell);

 protected:
  std::shared_ptr<AbstractPDGFColumn> _new_column_with_data_type(ColumnType type);
  std::shared_ptr<AbstractPDGFColumn> _new_non_generated_column_with_data_type(ColumnType type);

  ChunkOffset _hyrise_table_chunk_size;

  uint32_t _table_id;
  std::string _table_name;
  bool _table_will_be_generated;
  int64_t _table_num_rows;
  int64_t _received_rows = 0;

  std::vector<std::string> _table_column_names;
  std::vector<ColumnType> _table_column_types;
  std::vector<std::shared_ptr<AbstractPDGFColumn>> _table_columns;

  uint8_t _num_generated_columns;
  std::array<std::shared_ptr<AbstractPDGFColumn>, num_columns> _generated_columns;
};

template class PDGFTableBuilder<128u, 16u>;
} // namespace hyrise