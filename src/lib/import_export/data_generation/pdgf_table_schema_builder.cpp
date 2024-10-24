#include <algorithm>
#include <cstdint>
#include <cstddef>
#include <iostream>
#include <string>
#include <vector>

#include "boost/algorithm/string.hpp"

#include "non_generated_pdgf_column.hpp"
#include "pdgf_column.hpp"
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
    table_column_definitions.emplace_back(_table_column_names[i], _table_column_types[i], false);
  }

  auto table = std::make_shared<Table>(table_column_definitions, TableType::Data, _hyrise_table_chunk_size, UseMvcc::Yes);

  // Insert dummy segments
  auto all_columns_empty = std::vector<std::shared_ptr<AbstractPDGFColumn>>{};
  all_columns_empty.reserve(_table_column_types.size());
  for (auto type : _table_column_types) {
    all_columns_empty.emplace_back(_new_non_generated_column_with_data_type(type));
  }

  while (all_columns_empty[0]->has_another_segment()) {
    auto segments = Segments{};
    for (auto& column : all_columns_empty) {
      Assert(column->has_another_segment(), "All table columns should have the same number of segments!");
      segments.emplace_back(column->build_next_segment());
    }
    auto mvcc_data = std::make_shared<MvccData>(segments.front()->size(), CommitID{0});

    table->append_chunk(segments, mvcc_data);
  }

  return table;
}

template <uint32_t work_unit_size, uint32_t num_columns>
void PDGFTableSchemaBuilder<work_unit_size, num_columns>::read_schema(SharedMemoryDataCell<work_unit_size, num_columns>* schema_cell) {
  _table_name = std::string(schema_cell->data[1][0]);
  _table_num_rows = * reinterpret_cast<int64_t*>(schema_cell->data[2][0]);
  boost::algorithm::to_lower(_table_name);

  std::cerr << "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n";
  std::cerr << "TABLE ID " << _table_id << "\n";
  std::cerr << "NUM ROWS " << _table_num_rows << "\n";
  std::cerr << "TABLE NAME " << _table_name << "\n";
  std::cerr << "--- FIELDS OVERVIEW\n";
  // TODO(JEH): mention possible endianess problems in thesis
  auto table_num_columns = * reinterpret_cast<uint32_t*>(schema_cell->data[3][0]);
  for (uint32_t i = 0; i < table_num_columns; ++i) {
    auto column_name = std::string(schema_cell->data[4 + (2 * i)][0]);
    boost::algorithm::to_lower(column_name);
    auto pdgf_column_type = * reinterpret_cast<PDGFColumnType*>(schema_cell->data[5 + (2 * i)][0]);
    std::cerr << i << " " << column_name << " " << pdgf_column_type << "\n";

    _table_column_names.push_back(std::move(column_name));
    _table_column_types.push_back(hyrise_type_for_column_type(pdgf_column_type));
  }
}

template <uint32_t work_unit_size, uint32_t num_columns>
std::shared_ptr<AbstractPDGFColumn> PDGFTableSchemaBuilder<work_unit_size, num_columns>::_new_non_generated_column_with_data_type(DataType data_type) {
  std::shared_ptr<AbstractPDGFColumn> column;
  resolve_data_type(data_type, [&](auto type) {
    using ColumnDataType = typename decltype(type)::type;
    column = std::make_shared<NonGeneratedPDGFColumn<ColumnDataType>>(_table_num_rows, _hyrise_table_chunk_size);
  });
  return column;
}
} // namespace hyrise
