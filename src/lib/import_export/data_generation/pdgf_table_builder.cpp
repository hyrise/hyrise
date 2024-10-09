#include "boost/algorithm/string.hpp"

#include "pdgf_table_builder.hpp"

namespace hyrise {

template <uint32_t work_unit_size, uint32_t num_columns>
PDGFTableBuilder<work_unit_size, num_columns>::PDGFTableBuilder(uint32_t table_id, ChunkOffset hyrise_table_chunk_size, int64_t table_num_rows)
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
  // TODO: empty tables? do I need to at least generate dummy segments for all columns there?

  Assert(_table_columns.size() > 0, "Table schema should have at least one column!");

  // Assemble table metadata
  auto table_column_definitions = TableColumnDefinitions{};
  for (auto i = size_t{0}; i < _table_column_names.size(); ++i) {
    table_column_definitions.emplace_back(_table_column_names[i], hyrise_type_for_column_type(_table_column_types[i]), false);
  }
  auto table = std::make_shared<Table>(table_column_definitions, TableType::Data, _hyrise_table_chunk_size, UseMvcc::Yes);

  // Assemble table data
  while (_table_columns[0]->has_another_segment()) {
    auto segments = Segments{};
    for (auto& column : _table_columns) {
      Assert(column->has_another_segment(), "All table columns should have the same number of segments!");
      segments.emplace_back(column->build_next_segment());
    }
    auto mvcc_data = std::make_shared<MvccData>(segments.front()->size(), CommitID{0});

    table->append_chunk(segments, mvcc_data);
  }

  return table;
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
    boost::algorithm::to_lower(column_name);
    auto column_type = * reinterpret_cast<ColumnType*>(schema_cell->data[4 + (2 * i)][0]);
    std::cerr << i << " " << column_name << " " << column_type << "\n";

    _table_column_names.push_back(std::move(column_name));
    _table_column_types.push_back(column_type);
    _table_columns.emplace_back(std::make_shared<NonGeneratedPDGFColumn>(_table_num_rows, _hyrise_table_chunk_size));
  }
}

template <uint32_t work_unit_size, uint32_t num_columns>
void PDGFTableBuilder<work_unit_size, num_columns>::read_generation_info(SharedMemoryDataCell<work_unit_size, num_columns>* info_cell) {
  auto table_id = * reinterpret_cast<uint32_t*>(info_cell->data[0][0]);
  Assert(table_id == _table_id, "Trying to read generation info for a different table!");
  std::cerr << "--- READING GENERATION INFO\n";

  auto num_generated_columns = * reinterpret_cast<uint32_t*>(info_cell->data[1][0]);
  _num_generated_columns = static_cast<uint8_t>(num_generated_columns);
  for (auto i = uint8_t{0}; i < _num_generated_columns; ++i) {
    auto column_name = std::string(info_cell->data[2 + i][0]);
    boost::algorithm::to_lower(column_name);

    auto find = std::find(_table_column_names.begin(), _table_column_names.end(), column_name);
    Assert(find != _table_column_names.end(), "Trying to generate column " + column_name + " that does not belong to the table!");
    auto mapping_index = std::distance(_table_column_names.begin(), find);
    auto generated_column_type = _table_column_types[mapping_index];
    std::cerr << i << " " << column_name << " corresponds to index " << mapping_index << " (type " << generated_column_type << ")\n";
    _generated_columns[i] = _new_column_with_data_type(generated_column_type);

    // replace this column in the table columns
    _table_columns[mapping_index] = _generated_columns[i];
  }
}

template <uint32_t work_unit_size, uint32_t num_columns>
void PDGFTableBuilder<work_unit_size, num_columns>::read_data(uint32_t table_id, int64_t sorting_id,
                                              SharedMemoryDataCell<work_unit_size, num_columns>* data_cell) {
  Assert(table_id == _table_id, "Trying to append data to a table it does not belong to!");
  std::cout << "Reading #" << sorting_id << "\n";
  auto cell_rows = static_cast<size_t>(std::min(_table_num_rows - _received_rows, static_cast<int64_t>(work_unit_size)));
  for (auto row = size_t{0}; row < cell_rows; ++row) {
    for (auto col = uint8_t{0}; col < _num_generated_columns; ++col) {
      _generated_columns[col]->add(sorting_id * work_unit_size + row, data_cell->data[row][col]);
    }
  }
  _received_rows += cell_rows;
}

template <uint32_t work_unit_size, uint32_t num_columns>
std::shared_ptr<AbstractPDGFColumn> PDGFTableBuilder<work_unit_size, num_columns>::_new_column_with_data_type(ColumnType type) {
  switch (type) {
    case ColumnType::STRING:
      return std::make_shared<PDGFColumn<pmr_string>>(_table_num_rows, _hyrise_table_chunk_size);
    case BOOL:
    case INTEGER:
      return std::make_shared<PDGFColumn<int32_t>>(_table_num_rows, _hyrise_table_chunk_size);
    case LONG:
      return std::make_shared<PDGFColumn<int64_t>>(_table_num_rows, _hyrise_table_chunk_size);
    case DOUBLE:
      return std::make_shared<PDGFColumn<double>>(_table_num_rows, _hyrise_table_chunk_size);
    default:
      throw std::runtime_error("Unknown column type encountered!");
  }
}
} // namespace hyrise
