#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstddef>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "boost/algorithm/string.hpp"

#include "hyrise.hpp"
#include "non_generated_pdgf_column.hpp"
#include "pdgf_column.hpp"
#include "pdgf_table_schema_builder.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/job_task.hpp"
#include "storage/dummy_segment.hpp"
#include "utils/assert.hpp"
#include "utils/timer.hpp"


namespace hyrise {
template <uint32_t work_unit_size, uint32_t num_columns>
PDGFTableSchemaBuilder<work_unit_size, num_columns>::PDGFTableSchemaBuilder(uint32_t table_id, ChunkOffset hyrise_table_chunk_size)
    : _hyrise_table_chunk_size(hyrise_table_chunk_size), _table_id(table_id) {}

template <uint32_t work_unit_size, uint32_t num_columns>
std::string PDGFTableSchemaBuilder<work_unit_size, num_columns>::table_name() const {
  return _table_name;
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

    _table_columns.emplace_back(_new_non_generated_column_with_data_type(std::move(column_name), hyrise_type_for_column_type(pdgf_column_type)));
  }
}

template <uint32_t work_unit_size, uint32_t num_columns>
std::shared_ptr<Table> PDGFTableSchemaBuilder<work_unit_size, num_columns>::build_table() {
  Assert(!_table_columns.empty(), "Table schema should have at least one column!");
  std::cerr << "Creating empty table " << _table_name << "\n";
  auto timer = Timer{};
  auto table = _assemble_table_metadata();
  _construct_table_chunks(table);
  std::cerr << "Creating empty table " << _table_name << " done (" << timer.lap_formatted() << ")\n";
  return table;
}

template <uint32_t work_unit_size, uint32_t num_columns>
std::shared_ptr<Table> PDGFTableSchemaBuilder<work_unit_size, num_columns>::_assemble_table_metadata() {
  auto table_column_definitions = TableColumnDefinitions{};
  for (auto& column : _table_columns) {
    table_column_definitions.emplace_back(column->name(), column->type(), false, false);
  }

  return std::make_shared<Table>(table_column_definitions, TableType::Data, _hyrise_table_chunk_size, UseMvcc::Yes);
}

template <uint32_t work_unit_size, uint32_t num_columns>
void PDGFTableSchemaBuilder<work_unit_size, num_columns>::_construct_table_chunks(std::shared_ptr<Table>& table) {
  // Fill all chunks with dummy segments
  auto req_chunks = std::div(_table_num_rows, _hyrise_table_chunk_size);
  auto remaining_chunks_to_generate = std::atomic_int64_t{req_chunks.quot};
  auto table_access_mutex = std::mutex{};

  auto tasks = std::vector<std::shared_ptr<AbstractTask>>{};
  auto num_tasks = Hyrise::get().scheduler()->num_workers(); // this is a bit of a abstraction breaker, but it should do for now
  for (auto worker_index = uint32_t{0}; worker_index < num_tasks; worker_index++) {
    tasks.emplace_back(std::make_shared<JobTask>([this, &table, &table_access_mutex, &remaining_chunks_to_generate] {
      while (remaining_chunks_to_generate.fetch_sub(1) > 0) {
        auto segments = Segments{};
        for (auto& column : _table_columns) {
          segments.emplace_back(column->build_segment(_hyrise_table_chunk_size));
        }
        auto mvcc_data = std::make_shared<MvccData>(_hyrise_table_chunk_size, CommitID{0});

        table_access_mutex.lock();
        table->append_chunk(segments, mvcc_data);
        table_access_mutex.unlock();
      }
    }));
  }
  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(tasks);
  // Add remaining smaller chunk
  if (req_chunks.rem > 0) {
    auto segments = Segments{};
    for (auto& column : _table_columns) {
      segments.emplace_back(column->build_segment(static_cast<ChunkOffset>(req_chunks.rem)));
    }
    auto mvcc_data = std::make_shared<MvccData>(_hyrise_table_chunk_size, CommitID{0});
    table->append_chunk(segments, mvcc_data);
  }
}

template <uint32_t work_unit_size, uint32_t num_columns>
std::shared_ptr<BaseNonGeneratedPDGFColumn> PDGFTableSchemaBuilder<work_unit_size, num_columns>::_new_non_generated_column_with_data_type(std::string name, DataType data_type) {
  std::shared_ptr<BaseNonGeneratedPDGFColumn> column;
  resolve_data_type(data_type, [&](auto type) {
    using ColumnDataType = typename decltype(type)::type;
    column = std::make_shared<NonGeneratedPDGFColumn<ColumnDataType>>(name, data_type);
  });
  return column;
}

template class PDGFTableSchemaBuilder<   8u, 16u>; // 65536
template class PDGFTableSchemaBuilder<  16u, 16u>; // 32768
template class PDGFTableSchemaBuilder<  32u, 16u>; // 16384
template class PDGFTableSchemaBuilder<  64u, 16u>; //  8192
template class PDGFTableSchemaBuilder< 128u, 16u>; //  4096 buffer size
template class PDGFTableSchemaBuilder< 256u, 16u>; //  2048
template class PDGFTableSchemaBuilder< 512u, 16u>; //  1024
template class PDGFTableSchemaBuilder<1024u, 16u>; //   512
template class PDGFTableSchemaBuilder<2048u, 16u>; //   256
template class PDGFTableSchemaBuilder<4096u, 16u>; //   128
template class PDGFTableSchemaBuilder<8192u, 16u>; //    64

template class PDGFTableSchemaBuilder<   8u, 32u>; // 32768
template class PDGFTableSchemaBuilder<  16u, 32u>; // 16384
template class PDGFTableSchemaBuilder<  32u, 32u>; //  8192
template class PDGFTableSchemaBuilder<  64u, 32u>; //  4096
template class PDGFTableSchemaBuilder< 128u, 32u>; //  2048 buffer size
template class PDGFTableSchemaBuilder< 256u, 32u>; //  1024
template class PDGFTableSchemaBuilder< 512u, 32u>; //   512
template class PDGFTableSchemaBuilder<1024u, 32u>; //   256
template class PDGFTableSchemaBuilder<2048u, 32u>; //   128
template class PDGFTableSchemaBuilder<4096u, 32u>; //    64
template class PDGFTableSchemaBuilder<8192u, 32u>; //    32
} // namespace hyrise
