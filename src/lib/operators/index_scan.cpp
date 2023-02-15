#include "index_scan.hpp"

#include <algorithm>

#include "expression/between_expression.hpp"

#include "hyrise.hpp"

#include "scheduler/abstract_task.hpp"
#include "scheduler/job_task.hpp"

#include "storage/index/partial_hash/partial_hash_index.hpp"
#include "storage/reference_segment.hpp"

#include "utils/assert.hpp"

namespace hyrise {

IndexScan::IndexScan(const std::shared_ptr<const AbstractOperator>& input_operator,
                     const std::vector<ColumnID>& left_column_ids, const PredicateCondition predicate_condition,
                     const std::vector<AllTypeVariant>& right_values, const std::vector<AllTypeVariant>& right_values2)
    : AbstractReadOnlyOperator{OperatorType::IndexScan, input_operator},
      _left_column_ids{left_column_ids},
      _left_column_id{_left_column_ids[0]},
      _predicate_condition{predicate_condition},
      _right_values{right_values},
      _right_values2{right_values2} {}

const std::string& IndexScan::name() const {
  static const auto name = std::string{"IndexScan"};
  return name;
}

std::shared_ptr<const Table> IndexScan::_on_execute() {
  _in_table = left_input_table();

  _validate_input();

  _out_table = std::make_shared<Table>(_in_table->column_definitions(), TableType::References);

  std::mutex output_mutex;

  auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  if (included_chunk_ids.empty()) {
    const auto chunk_count = _in_table->chunk_count();
    jobs.reserve(chunk_count);
    for (auto chunk_id = ChunkID{0u}; chunk_id < chunk_count; ++chunk_id) {
      const auto chunk = _in_table->get_chunk(chunk_id);
      Assert(chunk, "Physically deleted chunk should not reach this point, see get_chunk / #1686.");

      jobs.push_back(_create_job(chunk_id, output_mutex));
    }
  } else {
    jobs.reserve(included_chunk_ids.size());
    for (auto chunk_id : included_chunk_ids) {
      if (_in_table->get_chunk(chunk_id)) {
        jobs.push_back(_create_job(chunk_id, output_mutex));
      }
    }
  }

  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);

  return _out_table;
}

std::shared_ptr<AbstractOperator> IndexScan::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_left_input,
    const std::shared_ptr<AbstractOperator>& copied_right_input,
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const {
  return std::make_shared<IndexScan>(copied_left_input, _left_column_ids, _predicate_condition,
                                     _right_values, _right_values2);
}

void IndexScan::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

std::shared_ptr<AbstractTask> IndexScan::_create_job(const ChunkID chunk_id, std::mutex& output_mutex) {
  auto job_task = std::make_shared<JobTask>([this, chunk_id, &output_mutex]() {
    // The output chunk is allocated on the same NUMA node as the input chunk.
    const auto chunk = _in_table->get_chunk(chunk_id);
    if (!chunk) {
      return;
    }

    const auto matches_out = std::make_shared<RowIDPosList>(_scan_chunk(chunk_id));
    if (matches_out->empty()) {
      return;
    }

    Segments segments;

    for (ColumnID column_id{0u}; column_id < _in_table->column_count(); ++column_id) {
      auto ref_segment_out = std::make_shared<ReferenceSegment>(_in_table, column_id, matches_out);
      segments.push_back(ref_segment_out);
    }

    std::lock_guard<std::mutex> lock(output_mutex);
    _out_table->append_chunk(segments, nullptr, chunk->get_allocator());
  });

  return job_task;
}

void IndexScan::_validate_input() {
  Assert(_predicate_condition != PredicateCondition::Like, "Predicate condition not supported by index scan.");
  Assert(_predicate_condition != PredicateCondition::NotLike, "Predicate condition not supported by index scan.");

  Assert(_left_column_ids.size() == _right_values.size(),
         "Count mismatch: left column IDs and right values don’t have same size.");
  if (is_between_predicate_condition(_predicate_condition)) {
    Assert(_left_column_ids.size() == _right_values2.size(),
           "Count mismatch: left column IDs and right values don’t have same size.");
  }

  Assert(_in_table->type() == TableType::Data, "IndexScan only supports persistent tables right now.");
}

RowIDPosList IndexScan::_scan_chunk(const ChunkID chunk_id) {
  // const auto to_row_id = [chunk_id](ChunkOffset chunk_offset) { return RowID{chunk_id, chunk_offset}; };

  PartialHashIndex::Iterator range_begin;
  // auto range_end = AbstractChunkIndex::Iterator{};

  auto matches_out = RowIDPosList{};

  const auto& indexes = _in_table->get_table_indexes();
  Assert(!indexes.empty(), "No indexes for the requested ColumnID available.");

  if (indexes.size() > 1) {
    PerformanceWarning("There are multiple indexes available, but only the first one is used.");
  }
  const auto& index = indexes.front();
  (void)index;

  // auto append_matches = [&probe_side_position, &probe_chunk_id, this](auto index_begin, auto index_end) {
  //     _append_matches_table_index(index_begin, index_end, probe_side_position.chunk_offset(), probe_chunk_id);
  // };

  auto append_matches = [&](const PartialHashIndex::Iterator& begin,
                                            const PartialHashIndex::Iterator& end) {
    auto current_iter = begin;
    DebugAssert(_in_table->type() == TableType::Data, "Cannot guarantee single chunk PosList for non-data tables.");
    matches_out.guarantee_single_chunk();

    const auto current_matches_size = matches_out.size();
    const auto final_matches_size = current_matches_size + static_cast<size_t>(std::distance(current_iter, end));
    matches_out.resize(final_matches_size);

    for (auto matches_position = current_matches_size; matches_position < final_matches_size; ++matches_position) {
      matches_out[matches_position] = *current_iter;
      ++current_iter;
    }
  };


  switch (_predicate_condition) {
    case PredicateCondition::Equals: {
      for (const auto& value: _right_values) {
        index->range_equals_with_iterators(append_matches, value);
      }
      break;
    }
    case PredicateCondition::NotEquals: {
      for (const auto& value: _right_values) {
        index->range_not_equals_with_iterators(append_matches, value);
      }
      break;
    }
    default:
      Fail("Unsupported comparison type encountered");
  }


  return matches_out;
}

}  // namespace hyrise
