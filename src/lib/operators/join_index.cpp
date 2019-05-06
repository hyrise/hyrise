#include "join_index.hpp"

#include <map>
#include <memory>
#include <numeric>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "all_type_variant.hpp"
#include "join_nested_loop.hpp"
#include "multi_predicate_join/multi_predicate_join_evaluator.hpp"
#include "resolve_type.hpp"
#include "storage/index/base_index.hpp"
#include "storage/segment_iterate.hpp"
#include "type_comparison.hpp"
#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"

namespace opossum {

/*
 * This is an index join implementation. It expects to find an index on the right column.
 * It can be used for all join modes except JoinMode::Cross.
 * For the remaining join types or if no index is found it falls back to a nested loop join.
 */

bool JoinIndex::supports(JoinMode join_mode, PredicateCondition predicate_condition, DataType left_data_type,
                         DataType right_data_type, bool secondary_predicates) {
  return !secondary_predicates;
}

JoinIndex::JoinIndex(const std::shared_ptr<const AbstractOperator>& left,
                     const std::shared_ptr<const AbstractOperator>& right, const JoinMode mode,
                     const OperatorJoinPredicate& primary_predicate,
                     const std::vector<OperatorJoinPredicate>& secondary_predicates)
    : AbstractJoinOperator(OperatorType::JoinIndex, left, right, mode, primary_predicate, secondary_predicates,
                           std::make_unique<JoinIndex::PerformanceData>()) {}

const std::string JoinIndex::name() const { return "JoinIndex"; }

std::shared_ptr<AbstractOperator> JoinIndex::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<JoinIndex>(copied_input_left, copied_input_right, _mode, _primary_predicate);
}

void JoinIndex::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

std::shared_ptr<const Table> JoinIndex::_on_execute() {
  Assert(supports(_mode, _primary_predicate.predicate_condition,
                  input_table_left()->column_data_type(_primary_predicate.column_ids.first),
                  input_table_right()->column_data_type(_primary_predicate.column_ids.second),
                  !_secondary_predicates.empty()),
         "JoinHash doesn't support these parameters");

  _output_table = _initialize_output_table();

  _perform_join();

  return _output_table;
}

void JoinIndex::_perform_join() {
  _right_matches.resize(input_table_right()->chunk_count());
  _left_matches.resize(input_table_left()->chunk_count());

  const auto is_semi_or_anti_join =
      _mode == JoinMode::Semi || _mode == JoinMode::AntiNullAsFalse || _mode == JoinMode::AntiNullAsTrue;

  const auto track_left_matches = _mode == JoinMode::FullOuter || _mode == JoinMode::Left || is_semi_or_anti_join;
  const auto track_right_matches = _mode == JoinMode::FullOuter || _mode == JoinMode::Right;

  if (track_left_matches) {
    for (ChunkID chunk_id_left = ChunkID{0}; chunk_id_left < input_table_left()->chunk_count(); ++chunk_id_left) {
      _left_matches[chunk_id_left].resize(input_table_left()->get_chunk(chunk_id_left)->size());
    }
  }
  if (track_right_matches) {
    for (ChunkID chunk_id_right = ChunkID{0}; chunk_id_right < input_table_right()->chunk_count(); ++chunk_id_right) {
      _right_matches[chunk_id_right].resize(input_table_right()->get_chunk(chunk_id_right)->size());
    }
  }

  _pos_list_left = std::make_shared<PosList>();
  _pos_list_right = std::make_shared<PosList>();

  auto pos_list_size_to_reserve =
      std::max(uint64_t{100}, std::min(input_table_left()->row_count(), input_table_right()->row_count()));

  _pos_list_left->reserve(pos_list_size_to_reserve);
  _pos_list_right->reserve(pos_list_size_to_reserve);

  auto& performance_data = static_cast<PerformanceData&>(*_performance_data);

  auto secondary_predicate_evaluator =
      MultiPredicateJoinEvaluator{*input_table_left(), *input_table_right(), _mode, {}};

  // Scan all chunks for right input
  for (ChunkID chunk_id_right = ChunkID{0}; chunk_id_right < input_table_right()->chunk_count(); ++chunk_id_right) {
    const auto chunk_right = input_table_right()->get_chunk(chunk_id_right);
    const auto indices = chunk_right->get_indices(std::vector<ColumnID>{_primary_predicate.column_ids.second});
    std::shared_ptr<BaseIndex> index = nullptr;

    if (!indices.empty()) {
      // We assume the first index to be efficient for our join
      // as we do not want to spend time on evaluating the best index inside of this join loop
      index = indices.front();
    }

    // Scan all chunks from left input
    if (index) {
      for (ChunkID chunk_id_left = ChunkID{0}; chunk_id_left < input_table_left()->chunk_count(); ++chunk_id_left) {
        const auto segment_left =
            input_table_left()->get_chunk(chunk_id_left)->get_segment(_primary_predicate.column_ids.first);

        segment_with_iterators(*segment_left, [&](auto it, const auto end) {
          _join_two_segments_using_index(it, end, chunk_id_left, chunk_id_right, index);
        });
      }
      performance_data.chunks_scanned_with_index++;
    } else {
      // Fall back to NestedLoopJoin
      const auto segment_right =
          input_table_right()->get_chunk(chunk_id_right)->get_segment(_primary_predicate.column_ids.second);
      for (ChunkID chunk_id_left = ChunkID{0}; chunk_id_left < input_table_left()->chunk_count(); ++chunk_id_left) {
        const auto segment_left =
            input_table_left()->get_chunk(chunk_id_left)->get_segment(_primary_predicate.column_ids.first);
        JoinNestedLoop::JoinParams params{*_pos_list_left,
                                          *_pos_list_right,
                                          _left_matches[chunk_id_left],
                                          _right_matches[chunk_id_right],
                                          track_left_matches,
                                          track_right_matches,
                                          _mode,
                                          _primary_predicate.predicate_condition,
                                          secondary_predicate_evaluator,
                                          !is_semi_or_anti_join};
        JoinNestedLoop::_join_two_untyped_segments(*segment_left, *segment_right, chunk_id_left, chunk_id_right,
                                                   params);
      }
      performance_data.chunks_scanned_without_index++;
    }
  }

  // For Full Outer and Left Join we need to add all unmatched rows for the left side
  if (_mode == JoinMode::Left || _mode == JoinMode::FullOuter) {
    for (ChunkID chunk_id_left = ChunkID{0}; chunk_id_left < input_table_left()->chunk_count(); ++chunk_id_left) {
      for (ChunkOffset chunk_offset{0}; chunk_offset < _left_matches[chunk_id_left].size(); ++chunk_offset) {
        if (!_left_matches[chunk_id_left][chunk_offset]) {
          _pos_list_left->emplace_back(RowID{chunk_id_left, chunk_offset});
          _pos_list_right->emplace_back(NULL_ROW_ID);
        }
      }
    }
  }

  // For Full Outer and Right Join we need to add all unmatched rows for the right side.
  if (_mode == JoinMode::FullOuter || _mode == JoinMode::Right) {
    for (ChunkID chunk_id{0}; chunk_id < _right_matches.size(); ++chunk_id) {
      for (ChunkOffset chunk_offset{0}; chunk_offset < _right_matches[chunk_id].size(); ++chunk_offset) {
        if (!_right_matches[chunk_id][chunk_offset]) {
          _pos_list_right->emplace_back(RowID{chunk_id, chunk_offset});
          _pos_list_left->emplace_back(NULL_ROW_ID);
        }
      }
    }
  }

  _pos_list_left->shrink_to_fit();
  _pos_list_right->shrink_to_fit();

  // Write PosLists for Semi/Anti Joins, which so far haven't written any results to the PosLists
  // We use `left_matches_by_chunk` to determine whether a tuple from the left side found a match.
  if (is_semi_or_anti_join) {
    const auto invert = _mode == JoinMode::AntiNullAsFalse || _mode == JoinMode::AntiNullAsTrue;

    for (auto chunk_id = ChunkID{0}; chunk_id < input_table_left()->chunk_count(); ++chunk_id) {
      const auto chunk_size = input_table_left()->get_chunk(chunk_id)->size();
      for (auto chunk_offset = ChunkOffset{0}; chunk_offset < chunk_size; ++chunk_offset) {
        if (_left_matches[chunk_id][chunk_offset] ^ invert) {
          _pos_list_left->emplace_back(chunk_id, chunk_offset);
        }
      }
    }
  }

  // write output chunks
  Segments output_segments;

  _write_output_segments(output_segments, input_table_left(), _pos_list_left);
  if (!is_semi_or_anti_join) {
    _write_output_segments(output_segments, input_table_right(), _pos_list_right);
  }

  _output_table->append_chunk(output_segments);

  if (performance_data.chunks_scanned_with_index < performance_data.chunks_scanned_without_index) {
    PerformanceWarning(
        std::string("Only ") + std::to_string(performance_data.chunks_scanned_with_index) + " of " +
        std::to_string(performance_data.chunks_scanned_with_index + performance_data.chunks_scanned_without_index) +
        " chunks scanned using an index");
  }
}

// join loop that joins two segments of two columns using an iterator for the left, and an index for the right
template <typename LeftIterator>
void JoinIndex::_join_two_segments_using_index(LeftIterator left_it, LeftIterator left_end, const ChunkID chunk_id_left,
                                               const ChunkID chunk_id_right, const std::shared_ptr<BaseIndex>& index) {
  for (; left_it != left_end; ++left_it) {
    const auto left_value = *left_it;
    if (left_value.is_null()) continue;

    auto range_begin = BaseIndex::Iterator{};
    auto range_end = BaseIndex::Iterator{};

    switch (_primary_predicate.predicate_condition) {
      case PredicateCondition::Equals: {
        range_begin = index->lower_bound({left_value.value()});
        range_end = index->upper_bound({left_value.value()});
        break;
      }
      case PredicateCondition::NotEquals: {
        // first, get all values less than the search value
        range_begin = index->cbegin();
        range_end = index->lower_bound({left_value.value()});

        _append_matches(range_begin, range_end, left_value.chunk_offset(), chunk_id_left, chunk_id_right);

        // set range for second half to all values greater than the search value
        range_begin = index->upper_bound({left_value.value()});
        range_end = index->cend();
        break;
      }
      case PredicateCondition::GreaterThan: {
        range_begin = index->cbegin();
        range_end = index->lower_bound({left_value.value()});
        break;
      }
      case PredicateCondition::GreaterThanEquals: {
        range_begin = index->cbegin();
        range_end = index->upper_bound({left_value.value()});
        break;
      }
      case PredicateCondition::LessThan: {
        range_begin = index->upper_bound({left_value.value()});
        range_end = index->cend();
        break;
      }
      case PredicateCondition::LessThanEquals: {
        range_begin = index->lower_bound({left_value.value()});
        range_end = index->cend();
        break;
      }
      default:
        Fail("Unsupported comparison type encountered");
    }

    _append_matches(range_begin, range_end, left_value.chunk_offset(), chunk_id_left, chunk_id_right);
  }
}

void JoinIndex::_append_matches(const BaseIndex::Iterator& range_begin, const BaseIndex::Iterator& range_end,
                                const ChunkOffset chunk_offset_left, const ChunkID chunk_id_left,
                                const ChunkID chunk_id_right) {
  const auto num_right_matches = std::distance(range_begin, range_end);

  if (num_right_matches == 0) {
    return;
  }

  // Remember the matches for outer joins
  if (_mode == JoinMode::Left || _mode == JoinMode::FullOuter) {
    _left_matches[chunk_id_left][chunk_offset_left] = true;
  }

  // we replicate the left value for each right value
  std::fill_n(std::back_inserter(*_pos_list_left), num_right_matches, RowID{chunk_id_left, chunk_offset_left});

  std::transform(range_begin, range_end, std::back_inserter(*_pos_list_right),
                 [chunk_id_right](ChunkOffset chunk_offset_right) {
                   return RowID{chunk_id_right, chunk_offset_right};
                 });

  if (_mode == JoinMode::FullOuter || _mode == JoinMode::Right) {
    std::for_each(range_begin, range_end, [this, chunk_id_right](ChunkOffset chunk_offset_right) {
      _right_matches[chunk_id_right][chunk_offset_right] = true;
    });
  }
}

void JoinIndex::_write_output_segments(Segments& output_segments, const std::shared_ptr<const Table>& input_table,
                                       std::shared_ptr<PosList> pos_list) {
  // Add segments from table to output chunk
  for (ColumnID column_id{0}; column_id < input_table->column_count(); ++column_id) {
    std::shared_ptr<BaseSegment> segment;

    if (input_table->type() == TableType::References) {
      if (input_table->chunk_count() > 0) {
        auto new_pos_list = std::make_shared<PosList>();

        ChunkID current_chunk_id{0};

        auto reference_segment = std::static_pointer_cast<const ReferenceSegment>(
            input_table->get_chunk(ChunkID{0})->get_segment(column_id));

        // de-reference to the correct RowID so the output can be used in a Multi Join
        for (const auto& row : *pos_list) {
          if (row.is_null()) {
            new_pos_list->push_back(NULL_ROW_ID);
            continue;
          }
          if (row.chunk_id != current_chunk_id) {
            current_chunk_id = row.chunk_id;

            reference_segment = std::dynamic_pointer_cast<const ReferenceSegment>(
                input_table->get_chunk(current_chunk_id)->get_segment(column_id));
          }
          new_pos_list->push_back((*reference_segment->pos_list())[row.chunk_offset]);
        }

        segment = std::make_shared<ReferenceSegment>(reference_segment->referenced_table(),
                                                     reference_segment->referenced_column_id(), new_pos_list);
      } else {
        // If there are no Chunks in the input_table, we can't deduce the Table that input_table is referencing to.
        // pos_list will contain only NULL_ROW_IDs anyway, so it doesn't matter which Table the ReferenceSegment that
        // we output is referencing. HACK, but works fine: we create a dummy table and let the ReferenceSegment ref
        // it.
        const auto dummy_table = Table::create_dummy_table(input_table->column_definitions());
        segment = std::make_shared<ReferenceSegment>(dummy_table, column_id, pos_list);
      }
    } else {
      segment = std::make_shared<ReferenceSegment>(input_table, column_id, pos_list);
    }

    output_segments.push_back(segment);
  }
}

void JoinIndex::_on_cleanup() {
  _output_table.reset();
  _pos_list_left.reset();
  _pos_list_right.reset();
  _left_matches.clear();
  _right_matches.clear();
}

void JoinIndex::PerformanceData::output_to_stream(std::ostream& stream, DescriptionMode description_mode) const {
  OperatorPerformanceData::output_to_stream(stream, description_mode);

  stream << (description_mode == DescriptionMode::SingleLine ? " / " : "\\n");
  stream << std::to_string(chunks_scanned_with_index) << " of "
         << std::to_string(chunks_scanned_with_index + chunks_scanned_without_index) << " chunks used an index";
}

}  // namespace opossum
