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
  _in_table = left_input_table();

  auto chunk_id_mapping = std::unordered_map<ChunkID, ChunkID>{};
  auto pruned_chunk_ids_set = std::unordered_set<ChunkID>{};

  // If the input operator is of the type GetTable, pruning must be considered.
  const auto& input_get_table = dynamic_pointer_cast<const GetTable>(left_input());
  Assert(input_get_table, "IndexScan needs a GetTable operator as input.");

  // If columns have been pruned, calculate the ColumnID that was originally indexed.
  const auto& pruned_column_ids = input_get_table->pruned_column_ids();
  _indexed_column_id = column_id_before_pruning(_indexed_column_id, pruned_column_ids);

  // If chunks have been pruned, calculate a mapping that maps the pruned ChunkIDs to the original ones.
  const auto& pruned_chunk_ids = input_get_table->pruned_chunk_ids();
  chunk_id_mapping = chunk_ids_after_pruning(_in_table->chunk_count() + pruned_chunk_ids.size(), pruned_chunk_ids);
  pruned_chunk_ids_set = std::unordered_set<ChunkID>{pruned_chunk_ids.begin(), pruned_chunk_ids.end()};

  // Remove all values from the mapping not present in the included ChunkIDs.
  for (auto iterator = chunk_id_mapping.begin(); iterator != chunk_id_mapping.end();) {
    if (!std::binary_search(included_chunk_ids.begin(), included_chunk_ids.end(), iterator->second)) {
      iterator = chunk_id_mapping.erase(iterator);
    } else {
      ++iterator;
    }
  }

  _out_table = std::make_shared<Table>(_in_table->column_definitions(), TableType::References);
  auto matches_out = std::make_shared<RowIDPosList>();  // TODO(anyone): assess if std::deque is more appropriate here

  const auto& indexes = _in_table->get_table_indexes(_indexed_column_id);
  Assert(!indexes.empty(), "No indexes for the requested ColumnID available.");

  Assert(indexes.size() == 1, "We do not support the handling of multiple indexes for the same column.");
  const auto& index = indexes.front();

  const auto append_matches = [&](const auto& begin, const auto& end) {
    for (auto current_iter = begin; current_iter != end; ++current_iter) {
      const auto mapped_chunk_id = chunk_id_mapping.find((*current_iter).chunk_id);

      if (mapped_chunk_id != chunk_id_mapping.end()) {
        matches_out->emplace_back(mapped_chunk_id->second, (*current_iter).chunk_offset);
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

  if (matches_out->empty()) {
    return _out_table;
  }

  // Split the output RowIDPosList per Chunk.

  const auto reserved_pos_list_size = std::max(size_t{4}, matches_out->size() / chunk_id_mapping.size());

  auto matches_out_per_chunk = std::vector<std::shared_ptr<RowIDPosList>>{};
  matches_out_per_chunk.emplace_back(std::make_shared<RowIDPosList>());
  matches_out_per_chunk.back()->guarantee_single_chunk();
  matches_out_per_chunk.back()->reserve(reserved_pos_list_size);

  auto current_chunk_id = (*matches_out)[0].chunk_id;
  for (const auto& match : *matches_out) {
    // Indexes are storing positions per value chunk-wise sorted. The following assert checks that this assumption
    // remains valid. In case the index's behavior is changed, creating a new output chunk whenever the referenced
    // chunk ID changes, should be revisited.
    // TODO(Martin): change to debug Assert once we passed the full CI.
    Assert(_predicate_condition != PredicateCondition::Equals || current_chunk_id <= match.chunk_id, "Unexpected order of positions during index traversal.");

    if (match.chunk_id == current_chunk_id) {
      matches_out_per_chunk.back()->emplace_back(match);
      continue;
    }

    matches_out_per_chunk.emplace_back(std::make_shared<RowIDPosList>());
    matches_out_per_chunk.back()->guarantee_single_chunk();
    matches_out_per_chunk.back()->reserve(reserved_pos_list_size);
    matches_out_per_chunk.back()->emplace_back(match);
    current_chunk_id = match.chunk_id;
  }

  const auto in_table_column_count = _in_table->column_count();
  for (const auto& matches : matches_out_per_chunk) {
    auto segments = Segments{};
    segments.reserve(in_table_column_count);

    for (auto column_id = ColumnID{0}; column_id < in_table_column_count; ++column_id) {
      segments.emplace_back(std::make_shared<ReferenceSegment>(_in_table, column_id, matches));
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
