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

  _validate_input();

  auto chunk_id_mapping = std::vector<std::optional<ChunkID>>{};
  auto pruned_chunk_ids_set = std::unordered_set<ChunkID>{};

  // If the input operator is of the type GetTable, pruning must be considered.
  const auto& input_get_table = dynamic_pointer_cast<const GetTable>(left_input());
  if (input_get_table) {
    // If columns have been pruned, calculate the ColumnID that was originally indexed.
    const auto& pruned_column_ids = input_get_table->pruned_column_ids();
    _indexed_column_id = column_id_before_pruning(_indexed_column_id, pruned_column_ids);

    // If chunks have been pruned, calculate a mapping that maps the pruned ChunkIDs to the original ones.
    const auto& pruned_chunk_ids = input_get_table->pruned_chunk_ids();
    chunk_id_mapping = chunk_ids_after_pruning(_in_table->chunk_count() + pruned_chunk_ids.size(), pruned_chunk_ids);
    pruned_chunk_ids_set = std::unordered_set<ChunkID>{pruned_chunk_ids.begin(), pruned_chunk_ids.end()};
  } else {
    chunk_id_mapping = chunk_ids_after_pruning(_in_table->chunk_count(), std::vector<ChunkID>{});
    Fail("I hope this do not happen.");
  }

  const auto included_chunk_ids_set = std::unordered_set<ChunkID>(included_chunk_ids.begin(), included_chunk_ids.end());

  _out_table = std::make_shared<Table>(_in_table->column_definitions(), TableType::References);
  auto matches_out = std::make_shared<RowIDPosList>();

  const auto& indexes = _in_table->get_table_indexes(_indexed_column_id);
  Assert(!indexes.empty(), "No indexes for the requested ColumnID available.");

  Assert(indexes.size() == 1, "We do not support the handling of multiple indexes for the same column.");
  const auto& index = indexes.front();

  const auto append_matches = [&](const auto& begin, const auto& end) {
    for (auto current_iter = begin; current_iter != end; ++current_iter) {
      if (included_chunk_ids_set.contains((*current_iter).chunk_id)) {
        matches_out->emplace_back(RowID{*chunk_id_mapping[(*current_iter).chunk_id], (*current_iter).chunk_offset});
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
      Fail("Unsupported comparison type, PartialHashIndex only supports Equals and NotEquals.");
  }

  if (matches_out->empty()) {
    return _out_table;
  }

  const auto first_chunk_id = (*matches_out)[0].chunk_id;
  auto single_chunk_guaranteed = true;
  for (const auto match : *matches_out) {
    if (match.chunk_id != first_chunk_id) {
      single_chunk_guaranteed = false;
    }
  }

  if (single_chunk_guaranteed) {
    matches_out->guarantee_single_chunk();
  }

  const auto in_table_column_count = _in_table->column_count();
  auto segments = Segments{};
  segments.reserve(in_table_column_count);
  for (auto column_id = ColumnID{0}; column_id < in_table_column_count; ++column_id) {
    segments.emplace_back(std::make_shared<ReferenceSegment>(_in_table, column_id, matches_out));
  }

  _out_table->append_chunk(segments, nullptr);

  return _out_table;
}

std::shared_ptr<AbstractOperator> IndexScan::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_left_input,
    const std::shared_ptr<AbstractOperator>& /*copied_right_input*/,
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& /*copied_ops*/) const {
  return std::make_shared<IndexScan>(copied_left_input, _indexed_column_id, _predicate_condition, _scan_value);
}

void IndexScan::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

void IndexScan::_validate_input() {
  Assert(_predicate_condition != PredicateCondition::Like, "Predicate condition not supported by index scan.");
  Assert(_predicate_condition != PredicateCondition::NotLike, "Predicate condition not supported by index scan.");

  Assert(_in_table->type() == TableType::Data, "IndexScan only supports persistent tables right now.");
}

}  // namespace hyrise
