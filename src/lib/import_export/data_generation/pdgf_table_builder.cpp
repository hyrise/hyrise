#include <algorithm>
#include <cstdint>
#include <cstddef>
#include <iostream>
#include <string>
#include <memory>
#include <functional>

#include "boost/algorithm/string.hpp"

#include "abstract_pdgf_column.hpp"
#include "benchmark_table_encoder.hpp"
#include "encoding_config.hpp"
#include "hyrise.hpp"
#include "non_generated_pdgf_column.hpp"
#include "pdgf_table_builder.hpp"
#include "pdgf_column.hpp"
#include "resolve_type.hpp"
#include "shared_memory_dto.hpp"
#include "storage/encoding_type.hpp"
#include "storage/mvcc_data.hpp"
#include "storage/table.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/timer.hpp"


namespace hyrise {
template <uint32_t work_unit_size, uint32_t num_columns>
PDGFTableBuilder<work_unit_size, num_columns>::PDGFTableBuilder(uint32_t table_id, ChunkOffset hyrise_table_chunk_size)
    : _hyrise_table_chunk_size(hyrise_table_chunk_size), _table_id(table_id) {
}

template <uint32_t work_unit_size, uint32_t num_columns>
bool PDGFTableBuilder<work_unit_size, num_columns>::reader_should_handle_another_work_unit() {
    const auto res = _remaining_rows_to_generate.fetch_sub(work_unit_size);
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
  std::cerr << "Building table " << _table_name << "...\n";
  auto timer = Timer{};

  if (_num_generated_columns == 0) {
    std::cerr << "Warning: Building table without a generated column!\n";
    return table;
  }

  table->set_use_mvcc(UseMvcc::Yes);

  // Assemble table data
  // Note that we have already generated empty chunks when loading the schema, so we just replace the segments now
  // instead of creating new ones.
  auto total_segments = _generated_columns[0]->num_segments();
  auto next_chunk_index = std::atomic_uint32_t{0};
  auto tasks = std::vector<std::shared_ptr<AbstractTask>>{};
  auto num_tasks = Hyrise::get().scheduler()->num_workers(); // this is a bit of a abstraction breaker, but it should do for now
  for (auto worker_index = uint32_t{0}; worker_index < num_tasks; worker_index++) {
    tasks.emplace_back(std::make_shared<JobTask>([this, &table, &next_chunk_index, &total_segments] {
      ChunkID chunk_index;
      while ((chunk_index = static_cast<ChunkID>(next_chunk_index.fetch_add(1))) < total_segments) {
        auto chunk = table->get_chunk(chunk_index);
        if (!chunk->has_mvcc_data() || chunk->mvcc_data()->size() != chunk->size()) {
          chunk->set_mvcc_data(std::make_shared<MvccData>(chunk->size(), CommitID{0}));
        }
        for (auto i = size_t{0}; i < _num_generated_columns; ++i) {
          auto& column = _generated_columns[i];
          auto table_column_index = _generated_column_mappings[i];
          Assert(column->num_segments() > chunk_index, "All table columns should have the same number of segments!");
          chunk->put_segment(table_column_index, column->obtain_segment(chunk_index));
        }
      }
    }));
  }
  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(tasks);

  for (auto i = size_t{0}; i < _num_generated_columns; ++i) {
    auto table_column_index = _generated_column_mappings[i];
    table->column_definitions()[table_column_index].loaded = true;
  }

  std::cerr << "Building table " << _table_name << " done (" << timer.lap_formatted() << ")\n";
  return table;
}

template <uint32_t work_unit_size, uint32_t num_columns>
void PDGFTableBuilder<work_unit_size, num_columns>::read_generation_info(SharedMemoryDataCell<work_unit_size, num_columns>* info_cell, const EncodingConfig& encoding_config) {
  auto table_id = * reinterpret_cast<uint32_t*>(info_cell->data[1][0]);
  Assert(table_id == _table_id, "Trying to read generation info for a different table!");
  std::cerr << "--- READING GENERATION INFO\n";
  _table_name = std::string(info_cell->data[2][0]);
  boost::algorithm::to_lower(_table_name);
  std::cerr << "TABLE NAME " << _table_name << "\n";
  _table_num_rows = * reinterpret_cast<int64_t*>(info_cell->data[3][0]);
  _remaining_rows_to_generate.store(_table_num_rows);

  // Retrieve information from already loaded schema table
  Assert(Hyrise::get().storage_manager.has_table(_table_name), "Expected table to be already registered with storage manager. Maybe the table schema was not loaded beforehand?");
  auto table = Hyrise::get().storage_manager.get_table(_table_name);
  const auto table_column_names = table->column_names();

  // Determine required encoding spec
  _encoding_spec = BenchmarkTableEncoder::get_required_chunk_encoding_spec(_table_name, table, encoding_config);

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
    _new_column_with_data_type(i, _encoding_spec[mapping_index], generated_column_type);
    _generated_column_mappings[i] = mapping_index;
  }
}

template <uint32_t work_unit_size, uint32_t num_columns>
void PDGFTableBuilder<work_unit_size, num_columns>::read_data(uint32_t table_id, int64_t sorting_id,
                                              SharedMemoryDataCell<work_unit_size, num_columns>* data_cell, uint32_t num_contained_rows) {
  DebugAssert(table_id == _table_id, "Trying to append data to a table it does not belong to!");
  DebugAssert(num_contained_rows <= work_unit_size, "Reported cells in rows is too large!");
  // When have originally assigned the work unit, we assumed it was full
  // Now, we might have to update our calculation.
  // Note that this might lead workers to terminate early, as they might have seen no rows remaining when asking
  // whether to handle more work units, but then we have climbed above 0 again here.
  // However, we are still guaranteed to read the full data, because the worker thread currently executing this function
  // will see the updated value on its next cycle.
  _remaining_rows_to_generate += work_unit_size - num_contained_rows;
  for (auto row = size_t{0}; row < num_contained_rows; ++row) {
    for (auto col = uint8_t{0}; col < _num_generated_columns; ++col) {
      _generated_columns[col]->virtual_add((sorting_id * work_unit_size) + row, data_cell->data[row][col]);
    }
  }
  for (auto col = uint8_t{0}; col < _num_generated_columns; ++col) {
    _generated_columns[col]->values_added(sorting_id * work_unit_size, num_contained_rows);
  }
}

template <uint32_t work_unit_size, uint32_t num_columns>
void PDGFTableBuilder<work_unit_size, num_columns>::_new_column_with_data_type(uint8_t target_index, SegmentEncodingSpec segment_encoding_spec, DataType data_type) {
  resolve_data_type(data_type, [&](auto type) {
    using ColumnDataType = typename decltype(type)::type;
    _generated_columns[target_index] = std::make_shared<PDGFColumn<ColumnDataType>>(data_type, segment_encoding_spec, _table_num_rows, _hyrise_table_chunk_size);
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

template class PDGFTableBuilder<   8u, 32u>; // 32768
template class PDGFTableBuilder<  16u, 32u>; // 16384
template class PDGFTableBuilder<  32u, 32u>; //  8192
template class PDGFTableBuilder<  64u, 32u>; //  4096
template class PDGFTableBuilder< 128u, 32u>; //  2048 buffer size
template class PDGFTableBuilder< 256u, 32u>; //  1024
template class PDGFTableBuilder< 512u, 32u>; //   512
template class PDGFTableBuilder<1024u, 32u>; //   256
template class PDGFTableBuilder<2048u, 32u>; //   128
template class PDGFTableBuilder<4096u, 32u>; //    64
template class PDGFTableBuilder<8192u, 32u>; //    32
} // namespace hyrise
