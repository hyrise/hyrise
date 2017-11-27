#include "index_scan.hpp"

#include <algorithm>

#include "scheduler/abstract_task.hpp"
#include "scheduler/current_scheduler.hpp"
#include "scheduler/job_task.hpp"

#include "storage/index/base_index.hpp"
#include "storage/reference_column.hpp"

#include "utils/assert.hpp"

namespace opossum {

IndexScan::IndexScan(std::shared_ptr<AbstractOperator> in, std::vector<ChunkID> chunk_ids,
                     const ColumnIndexType index_type, std::vector<ColumnID> left_column_ids, const ScanType scan_type,
                     std::vector<AllTypeVariant> right_values, std::vector<AllTypeVariant> right_values2)
    : AbstractReadOnlyOperator{in},
      _chunk_ids{chunk_ids},
      _index_type{index_type},
      _left_column_ids{left_column_ids},
      _scan_type{scan_type},
      _right_values{right_values},
      _right_values2{right_values2} {}

const std::string IndexScan::name() const { return "IndexScan"; }

std::shared_ptr<const Table> IndexScan::_on_execute() {
  _in_table = _input_table_left();

  _validate_input();

  _out_table = Table::create_with_layout_from(_in_table);

  std::mutex output_mutex;

  auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  jobs.reserve(_chunk_ids.size());

  for (auto chunk_id : _chunk_ids) {
    auto job_task = std::make_shared<JobTask>([&]() {
      const auto chunk_guard = _in_table->get_chunk_with_access_counting(chunk_id);

      const auto matches_out = std::make_shared<PosList>(_scan_chunk(chunk_id));

      // The output chunk is allocated on the same NUMA node as the input chunk. Also, the AccessCounter is
      // reused to track accesses of the output chunk. Accesses of derived chunks are counted towards the
      // original chunk.
      Chunk chunk_out(chunk_guard->get_allocator(), chunk_guard->access_counter());

      for (ColumnID column_id{0u}; column_id < _in_table->column_count(); ++column_id) {
        auto ref_column_out = std::make_shared<ReferenceColumn>(_in_table, column_id, matches_out);
        chunk_out.add_column(ref_column_out);
      }

      std::lock_guard<std::mutex> lock(output_mutex);
      _out_table->emplace_chunk(std::move(chunk_out));
    });

    jobs.push_back(job_task);
    job_task->schedule();
  }

  CurrentScheduler::wait_for_tasks(jobs);

  return _out_table;
}

void IndexScan::_on_cleanup() {}

void IndexScan::_validate_input() {
  Assert(_scan_type != ScanType::OpLike, "Scan type not supported by index scan.");
  Assert(_scan_type != ScanType::OpNotLike, "Scan type not supported by index scan.");

  Assert(_left_column_ids.size() == _right_values.size(),
         "Count mismatch: left column IDs and right values don’t have same size.");
  if (_scan_type == ScanType::OpBetween) {
    Assert(_left_column_ids.size() == _right_values2.size(),
           "Count mismatch: left column IDs and right values don’t have same size.");
  }

  Assert(_in_table->get_type() == TableType::Data, "IndexScan does only support persistent tables right now.");
}

PosList IndexScan::_scan_chunk(const ChunkID chunk_id) {
  const auto to_row_id = [chunk_id](ChunkOffset chunk_offset) { return RowID{chunk_id, chunk_offset}; };

  auto range_begin = BaseIndex::Iterator{};
  auto range_end = BaseIndex::Iterator{};

  const auto& chunk = _in_table->get_chunk(chunk_id);
  auto matches_out = PosList{};

  const auto index = chunk.get_index_for(_index_type, _left_column_ids);
  DebugAssert(index != nullptr, "Index of specified type not found for column (vector).");

  switch (_scan_type) {
    case ScanType::OpEquals: {
      range_begin = index->lower_bound(_right_values);
      range_end = index->upper_bound(_right_values);
      break;
    }
    case ScanType::OpNotEquals: {
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
    case ScanType::OpLessThan: {
      range_begin = index->cbegin();
      range_end = index->lower_bound(_right_values);
      break;
    }
    case ScanType::OpGreaterThanEquals: {
      range_begin = index->lower_bound(_right_values);
      range_end = index->cend();
      break;
    }
    case ScanType::OpLessThanEquals: {
      range_begin = index->cbegin();
      range_end = index->upper_bound(_right_values);
      break;
    }
    case ScanType::OpGreaterThan: {
      range_begin = index->upper_bound(_right_values);
      range_end = index->cend();
      break;
    }
    case ScanType::OpBetween: {
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
