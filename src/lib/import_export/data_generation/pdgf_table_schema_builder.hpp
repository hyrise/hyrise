#pragma once

#include <cstdint>
#include <string>
#include <memory>
#include <vector>
#include <array>

#include "all_type_variant.hpp"
#include "shared_memory_dto.hpp"
#include "non_generated_pdgf_column.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace hyrise {

template <uint32_t work_unit_size, uint32_t num_columns>
class PDGFTableSchemaBuilder : Noncopyable {
 public:
  explicit PDGFTableSchemaBuilder(uint32_t table_id, ChunkOffset hyrise_table_chunk_size);

  std::string table_name() const;
  void read_schema(SharedMemoryDataCell<work_unit_size, num_columns>* schema_cell);
  std::shared_ptr<Table> build_table();

 protected:
  std::shared_ptr<Table> _assemble_table_metadata();
  void _construct_table_chunks(std::shared_ptr<Table>& table);
  std::shared_ptr<BaseNonGeneratedPDGFColumn> _new_non_generated_column_with_data_type(std::string name, DataType data_type);

  ChunkOffset _hyrise_table_chunk_size;

  uint32_t _table_id;
  std::string _table_name;
  int64_t _table_num_rows;

  std::vector<std::shared_ptr<BaseNonGeneratedPDGFColumn>> _table_columns;
};

template class PDGFTableSchemaBuilder<128u, 16u>;
} // namespace hyrise