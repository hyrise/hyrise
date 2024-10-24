#include <algorithm>
#include <cstdint>
#include <cstddef>
#include <iostream>
#include <string>

#include "boost/algorithm/string.hpp"

#include "pdgf_table_schema_builder.hpp"
#include "utils/assert.hpp"


namespace hyrise {

template <uint32_t work_unit_size, uint32_t num_columns>
PDGFTableSchemaBuilder<work_unit_size, num_columns>::PDGFTableSchemaBuilder(uint32_t table_id, ChunkOffset hyrise_table_chunk_size)
    : _hyrise_table_chunk_size(hyrise_table_chunk_size), _table_id(table_id) {}

template <uint32_t work_unit_size, uint32_t num_columns>
std::string PDGFTableSchemaBuilder<work_unit_size, num_columns>::table_name() const {
  return _table_name;
}

template <uint32_t work_unit_size, uint32_t num_columns>
std::shared_ptr<Table> PDGFTableSchemaBuilder<work_unit_size, num_columns>::build_table() {
  Assert(!_table_column_names.empty(), "Table schema should have at least one column!");

  // Assemble table metadata
  auto table_column_definitions = TableColumnDefinitions{};
  for (auto i = size_t{0}; i < _table_column_names.size(); ++i) {
    table_column_definitions.emplace_back(_table_column_names[i], hyrise_type_for_column_type(_table_column_types[i]), false);
  }
  return std::make_shared<Table>(table_column_definitions, TableType::Data, _hyrise_table_chunk_size, UseMvcc::Yes);
}

template <uint32_t work_unit_size, uint32_t num_columns>
void PDGFTableSchemaBuilder<work_unit_size, num_columns>::read_schema(SharedMemoryDataCell<work_unit_size, num_columns>* schema_cell) {
  std::cerr << "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n";
  std::cerr << "TABLE ID " << _table_id << "\n";
//  std::cerr << "NUM ROWS " << _table_num_rows << "\n";

  _table_name = std::string(schema_cell->data[1][0]);
  boost::algorithm::to_lower(_table_name);
  std::cerr << "TABLE NAME " << _table_name << "\n";
//  _table_num_rows = * reinterpret_cast<int64_t*>(schema_cell->data[2][0]);
  std::cerr << "--- FIELDS OVERVIEW\n";
  // TODO(JEH): mention possible endianess problems in thesis
  auto table_num_columns = * reinterpret_cast<uint32_t*>(schema_cell->data[2][0]);
  for (uint32_t i = 0; i < table_num_columns; ++i) {
    auto column_name = std::string(schema_cell->data[3 + (2 * i)][0]);
    boost::algorithm::to_lower(column_name);
    auto column_type = * reinterpret_cast<PDGFColumnType*>(schema_cell->data[4 + (2 * i)][0]);
    std::cerr << i << " " << column_name << " " << column_type << "\n";

    _table_column_names.push_back(std::move(column_name));
    _table_column_types.push_back(column_type);
  }
}
} // namespace hyrise
