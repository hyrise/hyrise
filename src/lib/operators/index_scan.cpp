#include "index_scan.hpp"

#include <algorithm>

#include "expression/between_expression.hpp"

#include "hyrise.hpp"

#include "operators/get_table.hpp"

#include "scheduler/abstract_task.hpp"
#include "scheduler/job_task.hpp"

#include "storage/index/partial_hash/partial_hash_index.hpp"
#include "storage/reference_segment.hpp"

#include "utils/assert.hpp"
#include "utils/column_pruning_utils.hpp"

namespace hyrise {

IndexScan::IndexScan(const std::shared_ptr<const AbstractOperator>& input_operator, const ColumnID indexed_column_id,
                     const PredicateCondition predicate_condition, const AllTypeVariant scan_value)
    : AbstractReadOnlyOperator{OperatorType::IndexScan, input_operator},
      _indexed_column_id{indexed_column_id},
      _predicate_condition{predicate_condition},
      _scan_value{scan_value} {}

const std::string& IndexScan::name() const {
  static const auto name = std::string{"IndexScan"};
  return name;
}

std::shared_ptr<const Table> IndexScan::_on_execute() {
  Assert(!included_chunk_ids.empty(), "Index scan expects a non-emptpy list of chunks to process.");
  DebugAssert(std::is_sorted(included_chunk_ids.begin(), included_chunk_ids.end()),
              "Included ChunkIDs must be sorted.");

  _in_table = left_input_table();

  // If the input operator is of type GetTable (which we require), we get only the chunks forwarded to the index scan,
  // for which an index exists (or more precise, for which an index existed during optimization). Nonetheless, we still
  // access the GetTable node to check if more chunks have been pruned at runtime (this will be part of the master,
  // later in 2023).
  // TODO(Daniel): please help me phrasing that.
  const auto& input_get_table = std::dynamic_pointer_cast<const GetTable>(left_input());
  Assert(input_get_table, "IndexScan needs a GetTable operator as input.");

  // If columns have been pruned, calculate the ColumnID that was originally indexed.
  const auto& pruned_column_ids = input_get_table->pruned_column_ids();
  _indexed_column_id = column_id_before_pruning(_indexed_column_id, pruned_column_ids);

  // If chunks have been pruned, calculate a mapping that maps the pruned ChunkIDs to the original ones.
  const auto& pruned_chunk_ids = input_get_table->pruned_chunk_ids();
  auto chunk_count_to_scan = static_cast<int32_t>(_in_table->chunk_count());
  auto chunk_id_mapping = chunk_ids_after_pruning(_in_table->chunk_count() + pruned_chunk_ids.size(), pruned_chunk_ids);

  // Remove all values from the mapping not present in the included ChunkIDs.
  for (auto index = size_t{0}; index < chunk_id_mapping.size(); ++index) {
    const auto chunk_id = chunk_id_mapping[index];
    if (chunk_id != INVALID_CHUNK_ID &&
        !std::binary_search(included_chunk_ids.begin(), included_chunk_ids.end(), chunk_id)) {
      chunk_id_mapping[index] = INVALID_CHUNK_ID;
      --chunk_count_to_scan;
    }
  }

  _out_table = std::make_shared<Table>(_in_table->column_definitions(), TableType::References);

  DebugAssert(chunk_count_to_scan >= 0, "Excluded more chunks than available.");
  if (chunk_count_to_scan == 0) {
    // All chunks to scan have been pruned (can happen due to dynamic pruning).
    return _out_table;
  }

  auto pos_lists = std::vector<std::shared_ptr<RowIDPosList>>();
  pos_lists.emplace_back(std::make_shared<RowIDPosList>());

  const auto& indexes = _in_table->get_table_indexes(_indexed_column_id);
  Assert(!indexes.empty(), "No indexes for the requested ColumnID available.");

  Assert(indexes.size() == 1, "We do not support the handling of multiple indexes for the same column.");
  const auto& index = indexes.front();

  auto& current_append_pos_list = pos_lists.back();
  const auto append_matches = [&](const auto& begin, const auto& end) {
    for (auto current_iter = begin; current_iter != end; ++current_iter) {
      const auto mapped_chunk_id = chunk_id_mapping[(*current_iter).chunk_id];

      if (mapped_chunk_id != INVALID_CHUNK_ID) {
        // For equal predicates, the results are sorted by chunk. It is thus possible to emit single pos lists per
        // chunk and guaranteeing that only single chunks are referenced. We decided against this as we expect the
        // result sets of index scans to be tiny in most cases (single chunk guarantee might not pay off and we could
        // end up with many very small segments).
        current_append_pos_list->emplace_back(mapped_chunk_id, (*current_iter).chunk_offset);

        if (current_append_pos_list->size() > Chunk::DEFAULT_SIZE) {
          pos_lists.emplace_back(std::make_shared<RowIDPosList>());
          current_append_pos_list = pos_lists.back();
        }
      }
    }
  };

  switch (_predicate_condition) {
    case PredicateCondition::Equals: {
      index->range_equals_with_iterators(append_matches, _scan_value);
      break;
    }
    case PredicateCondition::NotEquals: {
      index->range_not_equals_with_iterators(append_matches, _scan_value);
      break;
    }
    default:
      Fail("Unsupported comparison type. Currently, Hyrise's secondary indexes only support Equals and NotEquals.");
  }

  if (pos_lists[0]->empty()) {
    return _out_table;
  }

  const auto in_table_column_count = _in_table->column_count();
  for (const auto& pos_list : pos_lists) {
    auto segments = Segments{};
    segments.reserve(in_table_column_count);

    for (auto column_id = ColumnID{0}; column_id < in_table_column_count; ++column_id) {
      segments.emplace_back(std::make_shared<ReferenceSegment>(_in_table, column_id, pos_list));
    }

    _out_table->append_chunk(segments, nullptr);
  }

  return _out_table;
}

std::shared_ptr<AbstractOperator> IndexScan::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_left_input,
    const std::shared_ptr<AbstractOperator>& /*copied_right_input*/,
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& /*copied_ops*/) const {
  return std::make_shared<IndexScan>(copied_left_input, _indexed_column_id, _predicate_condition, _scan_value);
}

void IndexScan::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

}  // namespace hyrise
