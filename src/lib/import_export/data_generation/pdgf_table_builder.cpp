#include "boost/algorithm/string.hpp"

#include "pdgf_table_builder.hpp"

namespace hyrise {

template <uint32_t work_unit_size, uint32_t num_columns>
PDGFTableBuilder<work_unit_size, num_columns>::PDGFTableBuilder(uint32_t table_id, uint32_t hyrise_table_chunk_size, int64_t table_num_rows)
    : _hyrise_table_chunk_size(hyrise_table_chunk_size), _table_id(table_id), _table_num_rows(table_num_rows) {}

template <uint32_t work_unit_size, uint32_t num_columns>
bool PDGFTableBuilder<work_unit_size, num_columns>::expects_more_data() {
    return _received_rows < _table_num_rows;
}

template <uint32_t work_unit_size, uint32_t num_columns>
std::string PDGFTableBuilder<work_unit_size, num_columns>::table_name() {
  return _table_name;
}

template <uint32_t work_unit_size, uint32_t num_columns>
std::shared_ptr<Table> PDGFTableBuilder<work_unit_size, num_columns>::build_table() {
  return {};
}

template <uint32_t work_unit_size, uint32_t num_columns>
void PDGFTableBuilder<work_unit_size, num_columns>::read_schema(SharedMemoryDataCell<work_unit_size, num_columns>* schema_cell) {
  std::cerr << "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n";
  std::cerr << "TABLE ID " << _table_id << "\n";
  std::cerr << "NUM ROWS " << _table_num_rows << "\n";

  _table_name = std::string(schema_cell->data[1][0]);
  boost::algorithm::to_lower(_table_name);
  std::cerr << "TABLE NAME " << _table_name << "\n";
  std::cerr << "--- FIELDS OVERVIEW\n";
  // TODO: mention possible endianess problems in thesis
  auto table_num_columns = * reinterpret_cast<uint32_t*>(schema_cell->data[2][0]);
  for (uint32_t i = 0; i < table_num_columns; ++i) {
    auto column_name = std::string(schema_cell->data[3 + (2 * i)][0]);
    boost::algorithm::to_lower(_table_name);
    auto column_type = * reinterpret_cast<ColumnType*>(schema_cell->data[4 + (2 * i)][0]);
    std::cerr << i << " " << column_name << " " << column_type << "\n";
    _table_column_names.push_back(std::move(column_name));
    _table_column_types.push_back(column_type);
  }
}

template <uint32_t work_unit_size, uint32_t num_columns>
void PDGFTableBuilder<work_unit_size, num_columns>::read_generation_info(SharedMemoryDataCell<work_unit_size, num_columns>* info_cell) {
  auto table_id = * reinterpret_cast<uint32_t*>(info_cell->data[0][0]);
  Assert(table_id == _table_id, "Trying to read generation info for a different table!");
  std::cerr << "--- READING GENERATION INFO\n";

  auto num_generated_columns = * reinterpret_cast<uint32_t*>(info_cell->data[1][0]);
  for (uint32_t i = 0; i < num_generated_columns; ++i) {
    auto column_name = std::string(info_cell->data[2 + i][0]);
    boost::algorithm::to_lower(_table_name);

    auto find = std::find(_table_column_names.begin(), _table_column_names.end(), column_name);
    Assert(find != _table_column_names.end(), "Trying to generate column " + column_name + " that does not belong to the table!");
    auto mapping_index = std::distance(_table_column_names.begin(), find);
    _generated_column_full_table_mappings[i] = mapping_index;
    _generated_column_types[i] = _table_column_types[mapping_index];
    std::cerr << i << " " << column_name << " corresponds to index " << mapping_index << " (type " << _generated_column_types[i] << ")\n";
  }
}

template <uint32_t work_unit_size, uint32_t num_columns>
void PDGFTableBuilder<work_unit_size, num_columns>::read_data(uint32_t table_id, int64_t sorting_id,
                                              SharedMemoryDataCell<work_unit_size, num_columns>* data_cell) {
  Assert(table_id == _table_id, "Trying to append data to a table it does not belong to!");
  // std::cout << "Sort #" << sorting_id << "\n";
  _received_rows += work_unit_size;
}
} // namespace hyrise
