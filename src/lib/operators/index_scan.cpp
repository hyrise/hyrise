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
#include "utils/pruning_utils.hpp"

namespace hyrise {

IndexScan::IndexScan(const std::shared_ptr<const AbstractOperator>& input_operator, const ColumnID left_column_id,
                     const PredicateCondition predicate_condition, const AllTypeVariant right_value,
                     const std::optional<std::vector<std::optional<ChunkID>>> chunk_id_mapping)
    : AbstractReadOnlyOperator{OperatorType::IndexScan, input_operator},
      _left_column_id{left_column_id},
      _predicate_condition{predicate_condition},
      _right_value{right_value},
      _chunk_id_mapping{chunk_id_mapping} {}

const std::string& IndexScan::name() const {
  static const auto name = std::string{"IndexScan"};
  return name;
}

std::shared_ptr<const Table> IndexScan::_on_execute() {
  _in_table = left_input_table();

  _validate_input();

  if (!_chunk_id_mapping) {
    auto chunk_id_mapping = std::vector<std::optional<ChunkID>>(_in_table->chunk_count());
    std::iota(chunk_id_mapping.begin(), chunk_id_mapping.end(), ChunkID{0});
    _chunk_id_mapping = chunk_id_mapping;
  }

  _out_table = std::make_shared<Table>(_in_table->column_definitions(), TableType::References);

  PartialHashIndex::Iterator range_begin;
  // auto range_end = AbstractChunkIndex::Iterator{};

  auto matches_out = std::make_shared<RowIDPosList>();

  const auto& indexes = _in_table->get_table_indexes(_left_column_id);
  Assert(!indexes.empty(), "No indexes for the requested ColumnID available.");

  Assert(indexes.size() == 1, "We do not support the handling of multiple indexes for the same column.");
  const auto index = indexes.front();

  auto append_matches = [&](const PartialHashIndex::Iterator& begin, const PartialHashIndex::Iterator& end) {
    for (auto current_iter = begin; current_iter != end; ++current_iter) {
      if ((*_chunk_id_mapping)[(*current_iter).chunk_id]) {
        matches_out->emplace_back(RowID{*(*_chunk_id_mapping)[(*current_iter).chunk_id], (*current_iter).chunk_offset});
      }
    }
  };

  switch (_predicate_condition) {
    case PredicateCondition::Equals: {
      index->range_equals_with_iterators(append_matches, _right_value);
      break;
    }
    case PredicateCondition::NotEquals: {
      index->range_not_equals_with_iterators(append_matches, _right_value);
      break;
    }
    default:
      Fail("Unsupported comparison type encountered");
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

  Segments segments;
  for (ColumnID column_id{0u}; column_id < _in_table->column_count(); ++column_id) {
    auto ref_segment_out = std::make_shared<ReferenceSegment>(_in_table, column_id, matches_out);
    segments.push_back(ref_segment_out);
  }
  _out_table->append_chunk(segments, nullptr);

  return _out_table;
}

std::shared_ptr<AbstractOperator> IndexScan::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_left_input,
    const std::shared_ptr<AbstractOperator>& /*copied_right_input*/,
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& /*copied_ops*/) const {
  return std::make_shared<IndexScan>(copied_left_input, _left_column_id, _predicate_condition, _right_value,
                                     _chunk_id_mapping);
}

void IndexScan::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

void IndexScan::_validate_input() {
  Assert(_predicate_condition != PredicateCondition::Like, "Predicate condition not supported by index scan.");
  Assert(_predicate_condition != PredicateCondition::NotLike, "Predicate condition not supported by index scan.");

  Assert(_in_table->type() == TableType::Data, "IndexScan only supports persistent tables right now.");
}

}  // namespace hyrise
