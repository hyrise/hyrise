#include "pdgf_table_builder.hpp"

namespace hyrise {

template <uint32_t work_unit_size, uint32_t num_columns>
PDGFTableBuilder<work_unit_size, num_columns>::PDGFTableBuilder(uint32_t table_id, uint32_t hyrise_table_chunk_size, int64_t table_num_rows)
    : _table_id(table_id), _hyrise_table_chunk_size(hyrise_table_chunk_size), _table_num_rows(table_num_rows) {}

template <uint32_t work_unit_size, uint32_t num_columns>
bool PDGFTableBuilder<work_unit_size, num_columns>::expects_more_data() {
    return _received_rows < _table_num_rows;
}

template <uint32_t work_unit_size, uint32_t num_columns>
std::shared_ptr<Table> PDGFTableBuilder<work_unit_size, num_columns>::construct_table() {
  return {};
}

template <uint32_t work_unit_size, uint32_t num_columns>
void PDGFTableBuilder<work_unit_size, num_columns>::read_schema(SharedMemoryDataCell<work_unit_size, num_columns>* schema_cell) {
  std::cerr << "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n";
  std::cerr << "TABLE ID " << _table_id << "\n";
  std::cerr << "NUM ROWS " << _table_num_rows << "\n";

  std::cerr << "TABLE NAME " << std::string(schema_cell->data[1][0]) << "\n";
  std::cerr << "FIELDS OVERVIEW\n";
  auto table_num_columns = *((uint32_t*)schema_cell->data[1][1]);
  for (uint32_t i = 0; i < table_num_columns; ++i) {
    std::cerr << i << " " << std::string(schema_cell->data[2][i]) << " "
              << *((uint32_t*)schema_cell->data[3][i]) << "\n";
  }
}

template <uint32_t work_unit_size, uint32_t num_columns>
void PDGFTableBuilder<work_unit_size, num_columns>::read_generation_info(SharedMemoryDataCell<work_unit_size, num_columns>* info_cell) {}

template <uint32_t work_unit_size, uint32_t num_columns>
void PDGFTableBuilder<work_unit_size, num_columns>::read_data(uint32_t table_id, int64_t sorting_id,
                                              SharedMemoryDataCell<work_unit_size, num_columns>* data_cell) {
  Assert(table_id == _table_id, "Trying to append data to a table it does not belong to!");
  // std::cout << "Sort #" << sorting_id << "\n";
  _received_rows += work_unit_size;
}
} // namespace hyrise
