#include <algorithm>
#include <cstdint>
#include <cstddef>
#include <iostream>
#include <string>
#include <memory>
#include <functional>

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
    : _hyrise_table_chunk_size(hyrise_table_chunk_size), _table_id(table_id) {
  // TODO: remove
  _num_rows_to_read_per_work_unit = work_unit_size;
}

template <uint32_t work_unit_size, uint32_t num_columns>
bool PDGFTableBuilder<work_unit_size, num_columns>::reader_should_handle_another_work_unit() {
    auto res = _remaining_work_units_to_read--; // Value will be decremented after reading.
    // All work units have been read (or are in progress of being read by still active reader threads,
    // so the current reader is done now.
    return res > 0;
}

template <uint32_t work_unit_size, uint32_t num_columns>
bool PDGFTableBuilder<work_unit_size, num_columns>::reading_should_be_parallelized() const {
  return (_table_num_rows / work_unit_size) > 64;
}

template <uint32_t work_unit_size, uint32_t num_columns>
std::string PDGFTableBuilder<work_unit_size, num_columns>::table_name() const {
  return _table_name;
}

template <uint32_t work_unit_size, uint32_t num_columns>
std::shared_ptr<Table> PDGFTableBuilder<work_unit_size, num_columns>::build_table() {
  // Table schema has already been generated. Just add the data here.
  auto table = Hyrise::get().storage_manager.get_table(_table_name);

  if (_num_generated_columns == 0) {
    std::cerr << "Warning: Building table without a generated column!\n";
    return table;
  }

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
  _remaining_work_units_to_read.store((_table_num_rows + work_unit_size - 1) / work_unit_size); // ceil(_table_num_rows // work_unit_size)

  // Retrieve information from already loaded schema table
  Assert(Hyrise::get().storage_manager.has_table(_table_name), "Expected table to be already registered with storage manager. Maybe the table schema was not loaded beforehand?");
  auto table = Hyrise::get().storage_manager.get_table(_table_name);
  const auto table_column_names = table->column_names();

  // Setup generation
   auto num_generated_columns = * reinterpret_cast<uint32_t*>(info_cell->data[4][0]);
  _num_generated_columns = static_cast<uint8_t>(num_generated_columns);
//  _num_generated_columns = 1;
  auto to_reduce = 0;
  for (auto i = uint8_t{0}; i < _num_generated_columns; ++i) {
    auto column_name = std::string(info_cell->data[5 + i][0]);
    boost::algorithm::to_lower(column_name);

    auto find = std::find(table_column_names.begin(), table_column_names.end(), column_name);
    Assert(find != table_column_names.end(), "Trying to generate column " + column_name + " that does not belong to the table!");
    auto mapping_index = std::distance(table_column_names.begin(), find);
    auto generated_column_type = table->column_data_type(static_cast<ColumnID>(mapping_index));
//    if (generated_column_type == DataType::String) {
//      to_reduce++;
//      continue;
//    }
    std::cerr << static_cast<uint32_t>(i) << " " << column_name << " corresponds to index " << mapping_index << " (type " << generated_column_type << ")\n";
    _new_column_with_data_type(i - to_reduce, generated_column_type);
    _generated_column_mappings[i - to_reduce] = mapping_index;
  }
  _num_generated_columns -= to_reduce;
}

template <uint32_t work_unit_size, uint32_t num_columns>
void PDGFTableBuilder<work_unit_size, num_columns>::read_data(uint32_t table_id, int64_t sorting_id,
                                              SharedMemoryDataCell<work_unit_size, num_columns>* data_cell) {
  Assert(table_id == _table_id, "Trying to append data to a table it does not belong to!");
  // std::cout << "Reading #" << sorting_id << "\n";
  auto cell_rows = static_cast<size_t>(std::min(
      // We cannot use received rows here in case we receive entries out of order from PDGF
      // The problem is that the last data_cell might contain fewer data than the previous ones, but if we
      // determine the rows in this cell depending on the rows we have received so far, we might think that the last cell
      // still has work_unit_size rows if we receive it before a cell with a lower sorting_id.
      // Using the sorting_id allows us to reliably determine this.
      _table_num_rows - (sorting_id * work_unit_size),
      static_cast<int64_t>(_num_rows_to_read_per_work_unit)
      ));
  for (auto row = size_t{0}; row < cell_rows; ++row) {
    for (auto col = uint8_t{0}; col < _num_generated_columns; ++col) {
      _generated_columns[col]->virtual_add((sorting_id * work_unit_size) + row, data_cell->data[row][col]);
      // _add_methods[col](_generated_columns[col], (sorting_id * work_unit_size) + row, data_cell->data[row][col]);
    }
  }
}

template <uint32_t work_unit_size, uint32_t num_columns>
void PDGFTableBuilder<work_unit_size, num_columns>::_new_column_with_data_type(uint8_t target_index, DataType data_type) {
  resolve_data_type(data_type, [&](auto type) {
    using ColumnDataType = typename decltype(type)::type;
    _add_methods[target_index] = &PDGFColumn<ColumnDataType>::call_add;
    _generated_columns[target_index] = std::make_shared<PDGFColumn<ColumnDataType>>(_table_num_rows, _hyrise_table_chunk_size);
  });
}

template class PDGFTableBuilder<   8u, 16u>; // 65536
template class PDGFTableBuilder<  16u, 16u>; // 32768
template class PDGFTableBuilder<  32u, 16u>; // 16384
template class PDGFTableBuilder<  64u, 16u>; //  8192
template class PDGFTableBuilder< 128u, 16u>; //  4096 buffer size
template class PDGFTableBuilder< 256u, 16u>; //  2048
template class PDGFTableBuilder< 512u, 16u>; //  1024
template class PDGFTableBuilder<1024u, 16u>; //   512
template class PDGFTableBuilder<2048u, 16u>; //   256
template class PDGFTableBuilder<4096u, 16u>; //   128
template class PDGFTableBuilder<8192u, 16u>; //    64
} // namespace hyrise
