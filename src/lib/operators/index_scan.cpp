#include "index_scan.hpp"

#include <algorithm>

#include "scheduler/abstract_task.hpp"
#include "scheduler/current_scheduler.hpp"
#include "scheduler/job_task.hpp"

#include "storage/index/base_index.hpp"
#include "storage/reference_column.hpp"

#include "utils/assert.hpp"

namespace opossum {

IndexScan::IndexScan(std::shared_ptr<AbstractOperator> in, const ColumnIndexType index_type,
                     std::vector<ColumnID> left_column_ids, const ScanType scan_type,
                     std::vector<AllTypeVariant> right_values, std::vector<AllTypeVariant> right_values2)
    : AbstractReadOnlyOperator{in},
      _index_type{index_type},
      _left_column_ids{left_column_ids},
      _scan_type{scan_type},
      _right_values{right_values},
      _right_values2{right_values2} {}

const std::string IndexScan::name() const { return "IndexScan"; }

void IndexScan::set_included_chunk_ids(const std::vector<ChunkID>& chunk_ids) { _included_chunk_ids = chunk_ids; }

std::shared_ptr<const Table> IndexScan::_on_execute() {
  _in_table = _input_table_left();

  _validate_input();

  _out_table = Table::create_with_layout_from(_in_table);

  std::mutex output_mutex;

  auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  if (_included_chunk_ids.empty()) {
    jobs.reserve(_in_table->chunk_count());
    for (auto chunk_id = ChunkID{0u}; chunk_id < _in_table->chunk_count(); ++chunk_id) {
      jobs.push_back(_create_job_and_schedule(chunk_id, output_mutex));
    }
  } else {
    jobs.reserve(_included_chunk_ids.size());
    for (auto chunk_id : _included_chunk_ids) {
      jobs.push_back(_create_job_and_schedule(chunk_id, output_mutex));
    }
  }

  CurrentScheduler::wait_for_tasks(jobs);

  return _out_table;
}

std::shared_ptr<JobTask> IndexScan::_create_job_and_schedule(const ChunkID chunk_id, std::mutex& output_mutex) {
  auto job_task = std::make_shared<JobTask>([=, &output_mutex]() {
    const auto matches_out = std::make_shared<PosList>(_scan_chunk(chunk_id));

    const auto chunk = _in_table->get_chunk(chunk_id);
    // The output chunk is allocated on the same NUMA node as the input chunk. Also, the AccessCounter is
    // reused to track accesses of the output chunk. Accesses of derived chunks are counted towards the
    // original chunk.
    auto chunk_out = std::make_shared<Chunk>(chunk->get_allocator(), chunk->access_counter());

    for (ColumnID column_id{0u}; column_id < _in_table->column_count(); ++column_id) {
      auto ref_column_out = std::make_shared<ReferenceColumn>(_in_table, column_id, matches_out);
      chunk_out->add_column(ref_column_out);
    }

    std::lock_guard<std::mutex> lock(output_mutex);
    _out_table->emplace_chunk(std::move(chunk_out));
  });

  job_task->schedule();
  return job_task;
}

void IndexScan::_validate_input() {
  Assert(_scan_type != ScanType::Like, "Scan type not supported by index scan.");
  Assert(_scan_type != ScanType::NotLike, "Scan type not supported by index scan.");

  Assert(_left_column_ids.size() == _right_values.size(),
         "Count mismatch: left column IDs and right values don’t have same size.");
  if (_scan_type == ScanType::Between) {
    Assert(_left_column_ids.size() == _right_values2.size(),
           "Count mismatch: left column IDs and right values don’t have same size.");
  }

  Assert(_in_table->get_type() == TableType::Data, "IndexScan only supports persistent tables right now.");
}

PosList IndexScan::_scan_chunk(const ChunkID chunk_id) {
  const auto to_row_id = [chunk_id](ChunkOffset chunk_offset) { return RowID{chunk_id, chunk_offset}; };

  auto range_begin = BaseIndex::Iterator{};
  auto range_end = BaseIndex::Iterator{};

  const auto chunk = _in_table->get_chunk_with_access_counting(chunk_id);
  auto matches_out = PosList{};

  const auto index = chunk->get_index(_index_type, _left_column_ids);
  Assert(index != nullptr, "Index of specified type not found for column (vector).");

  switch (_scan_type) {
    case ScanType::Equals: {
      range_begin = index->lower_bound(_right_values);
      range_end = index->upper_bound(_right_values);
      break;
    }
    case ScanType::NotEquals: {
      // first, get all values less than the search value
      range_begin = index->cbegin();
      range_end = index->lower_bound(_right_values);

      matches_out.reserve(std::distance(range_begin, range_end));
      std::transform(range_begin, range_end, std::back_inserter(matches_out), to_row_id);

      // set range for second half to all values greater than the search value
      range_begin = index->upper_bound(_right_values);
      range_end = index->cend();
      break;
    }
    case ScanType::LessThan: {
      range_begin = index->cbegin();
      range_end = index->lower_bound(_right_values);
      break;
    }
    case ScanType::LessThanEquals: {
      range_begin = index->cbegin();
      range_end = index->upper_bound(_right_values);
      break;
    }
    case ScanType::GreaterThan: {
      range_begin = index->upper_bound(_right_values);
      range_end = index->cend();
      break;
    }
    case ScanType::GreaterThanEquals: {
      range_begin = index->lower_bound(_right_values);
      range_end = index->cend();
      break;
    }
    case ScanType::Between: {
      range_begin = index->lower_bound(_right_values);
      range_end = index->upper_bound(_right_values2);
      break;
    }
    default:
      Fail("Unsupported comparison type encountered");
  }

  matches_out.reserve(matches_out.size() + std::distance(range_begin, range_end));
  std::transform(range_begin, range_end, std::back_inserter(matches_out), to_row_id);

  return matches_out;
}

}  // namespace opossum
