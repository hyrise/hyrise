#include <algorithm>
#include <cstdint>
#include <cstddef>
#include <iostream>
#include <string>
#include <memory>

#include "boost/algorithm/string.hpp"

#include "abstract_pdgf_column.hpp"
#include "hyrise.hpp"
#include "non_generated_pdgf_column.hpp"
#include "pdgf_table_builder.hpp"
#include "pdgf_column.hpp"
#include "resolve_type.hpp"
#include "shared_memory_dto.hpp"
#include "storage/mvcc_data.hpp"
#include "storage/table.hpp"
#include "types.hpp"
#include "utils/assert.hpp"


namespace hyrise {

template <uint32_t work_unit_size, uint32_t num_columns>
PDGFTableBuilder<work_unit_size, num_columns>::PDGFTableBuilder(uint32_t table_id, ChunkOffset hyrise_table_chunk_size)
    : _hyrise_table_chunk_size(hyrise_table_chunk_size), _table_id(table_id) {}

template <uint32_t work_unit_size, uint32_t num_columns>
bool PDGFTableBuilder<work_unit_size, num_columns>::expects_more_data() const {
    return _received_rows < _table_num_rows;
}

template <uint32_t work_unit_size, uint32_t num_columns>
std::string PDGFTableBuilder<work_unit_size, num_columns>::table_name() const {
  return _table_name;
}

template <uint32_t work_unit_size, uint32_t num_columns>
std::shared_ptr<Table> PDGFTableBuilder<work_unit_size, num_columns>::build_table() {
  Assert(!_generated_columns.empty(), "Table schema should have at least one column!");

  // Table schema has already been generated. Just add the data here.
  auto table = Hyrise::get().storage_manager.get_table(_table_name);

  // Assemble table data
  // Note that we have already generated empty chunks when loading the schema, so we just replace the segments now
  // instead of creating new ones.
  auto chunk_index = ChunkID{0};
  while (_generated_columns[0]->has_another_segment()) {
    auto chunk = table->get_chunk(chunk_index);
    for (auto i = size_t{0}; i < _num_generated_columns; ++i) {
      auto& column = _generated_columns[i];
      auto table_column_index = _generated_column_mappings[i];
      Assert(column->has_another_segment(), "All table columns should have the same number of segments!");
      chunk->put_segment(table_column_index, column->build_next_segment());
    }
    chunk_index++;
  }

  return table;
}

template <uint32_t work_unit_size, uint32_t num_columns>
void PDGFTableBuilder<work_unit_size, num_columns>::read_generation_info(SharedMemoryDataCell<work_unit_size, num_columns>* info_cell) {
  auto table_id = * reinterpret_cast<uint32_t*>(info_cell->data[1][0]);
  Assert(table_id == _table_id, "Trying to read generation info for a different table!");
  std::cerr << "--- READING GENERATION INFO\n";
  _table_name = std::string(info_cell->data[2][0]);
  boost::algorithm::to_lower(_table_name);
  std::cerr << "TABLE NAME " << _table_name << "\n";
  _table_num_rows = * reinterpret_cast<int64_t*>(info_cell->data[3][0]);

  // Retrieve information from already loaded schema table
  Assert(Hyrise::get().storage_manager.has_table(_table_name), "Expected table to be already registered with storage manager. Maybe the table schema was not loaded beforehand?");
  auto table = Hyrise::get().storage_manager.get_table(_table_name);
  const auto table_column_names = table->column_names();

  // Setup generation
  auto num_generated_columns = * reinterpret_cast<uint32_t*>(info_cell->data[4][0]);
  _num_generated_columns = static_cast<uint8_t>(num_generated_columns);
  for (auto i = uint8_t{0}; i < _num_generated_columns; ++i) {
    auto column_name = std::string(info_cell->data[5 + i][0]);
    boost::algorithm::to_lower(column_name);

    auto find = std::find(table_column_names.begin(), table_column_names.end(), column_name);
    Assert(find != table_column_names.end(), "Trying to generate column " + column_name + " that does not belong to the table!");
    auto mapping_index = std::distance(table_column_names.begin(), find);
    auto generated_column_type = table->column_data_type(static_cast<ColumnID>(mapping_index));
    std::cerr << static_cast<uint32_t>(i) << " " << column_name << " corresponds to index " << mapping_index << " (type " << generated_column_type << ")\n";
    _generated_columns[i] = _new_column_with_data_type(generated_column_type);
    _generated_column_mappings[i] = mapping_index;
  }
}

template <uint32_t work_unit_size, uint32_t num_columns>
void PDGFTableBuilder<work_unit_size, num_columns>::read_data(uint32_t table_id, int64_t sorting_id,
                                              SharedMemoryDataCell<work_unit_size, num_columns>* data_cell) {
  Assert(table_id == _table_id, "Trying to append data to a table it does not belong to!");
  // std::cout << "Reading #" << sorting_id << "\n";
  auto cell_rows = static_cast<size_t>(std::min(_table_num_rows - _received_rows, static_cast<int64_t>(work_unit_size)));
  for (auto row = size_t{0}; row < cell_rows; ++row) {
    for (auto col = uint8_t{0}; col < _num_generated_columns; ++col) {
      _generated_columns[col]->add((sorting_id * work_unit_size) + row, data_cell->data[row][col]);
    }
  }
  _received_rows += cell_rows;
}

template <uint32_t work_unit_size, uint32_t num_columns>
std::shared_ptr<AbstractPDGFColumn> PDGFTableBuilder<work_unit_size, num_columns>::_new_column_with_data_type(DataType data_type) {
  std::shared_ptr<AbstractPDGFColumn> column;
  resolve_data_type(data_type, [&](auto type) {
    using ColumnDataType = typename decltype(type)::type;
    column = std::make_shared<PDGFColumn<ColumnDataType>>(_table_num_rows, _hyrise_table_chunk_size);
  });
  return column;
}
} // namespace hyrise
