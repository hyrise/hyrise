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
class PDGFTableSchemaBuilder : Noncopyable {
 public:
  explicit PDGFTableSchemaBuilder(uint32_t table_id, ChunkOffset hyrise_table_chunk_size);

  std::string table_name() const;
  std::shared_ptr<Table> build_table();

  void read_schema(SharedMemoryDataCell<work_unit_size, num_columns>* schema_cell);

 protected:
  ChunkOffset _hyrise_table_chunk_size;

  uint32_t _table_id;
  std::string _table_name;

  std::vector<std::string> _table_column_names;
  std::vector<PDGFColumnType> _table_column_types;
};

template class PDGFTableSchemaBuilder<128u, 16u>;
} // namespace hyrise