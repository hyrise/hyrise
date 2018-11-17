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
#include "resolve_type.hpp"
#include "storage/create_iterable_from_segment.hpp"
#include "storage/index/base_index.hpp"
#include "type_comparison.hpp"
#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"

namespace opossum {

/*
 * This is an index join implementation. It expects to find an index on the right column.
 * It can be used for all join modes except JoinMode::Cross.
 * For the remaining join types or if no index is found it falls back to a nested loop join.
 */

JoinIndex::JoinIndex(const std::shared_ptr<const AbstractOperator>& left,
                     const std::shared_ptr<const AbstractOperator>& right, const JoinMode mode,
                     const std::pair<ColumnID, ColumnID>& column_ids, const PredicateCondition predicate_condition)
    : AbstractJoinOperator(OperatorType::JoinIndex, left, right, mode, column_ids, predicate_condition,
                           std::make_unique<JoinIndex::PerformanceData>()) {
  DebugAssert(mode != JoinMode::Cross, "Cross Join is not supported by index join.");
}

const std::string JoinIndex::name() const { return "JoinIndex"; }

std::shared_ptr<AbstractOperator> JoinIndex::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<JoinIndex>(copied_input_left, copied_input_right, _mode, _column_ids, _predicate_condition);
}

void JoinIndex::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

std::shared_ptr<const Table> JoinIndex::_on_execute() {
  _create_table_structure();

  _perform_join();

  return _output_table;
}

void JoinIndex::_create_table_structure() {
  _left_in_table = _input_left->get_output();
  _right_in_table = _input_right->get_output();

  _left_column_id = _column_ids.first;
  _right_column_id = _column_ids.second;

  const bool left_may_produce_null = (_mode == JoinMode::Right || _mode == JoinMode::Outer);
  const bool right_may_produce_null = (_mode == JoinMode::Left || _mode == JoinMode::Outer);

  // Preparing output table by adding columns from left and right table
  TableColumnDefinitions column_definitions;
  auto add_column_definitions = [&](auto from_table, bool from_may_produce_null) {
    for (ColumnID column_id{0}; column_id < from_table->column_count(); ++column_id) {
      auto nullable = (from_may_produce_null || from_table->column_is_nullable(column_id));
      column_definitions.emplace_back(from_table->column_name(column_id), from_table->column_data_type(column_id),
                                      nullable);
    }
  };

  add_column_definitions(_left_in_table, left_may_produce_null);
  add_column_definitions(_right_in_table, right_may_produce_null);

  _output_table = std::make_shared<Table>(column_definitions, TableType::References);
}

void JoinIndex::_perform_join() {
  _right_matches.resize(_right_in_table->chunk_count());
  _left_matches.resize(_left_in_table->chunk_count());

  const auto track_left_matches = (_mode == JoinMode::Left || _mode == JoinMode::Outer);
  if (track_left_matches) {
    for (ChunkID chunk_id_left = ChunkID{0}; chunk_id_left < _left_in_table->chunk_count(); ++chunk_id_left) {
      // initialize the data structures for left matches
      _left_matches[chunk_id_left].resize(_left_in_table->get_chunk(chunk_id_left)->size());
    }
  }

  const auto track_right_matches = (_mode == JoinMode::Right || _mode == JoinMode::Outer);

  _pos_list_left = std::make_shared<PosList>();
  _pos_list_right = std::make_shared<PosList>();

  size_t worst_case = _left_in_table->row_count() * _right_in_table->row_count();

  _pos_list_left->reserve(worst_case);
  _pos_list_right->reserve(worst_case);

  auto& performance_data = static_cast<PerformanceData&>(*_performance_data);

  // Scan all chunks for right input
  for (ChunkID chunk_id_right = ChunkID{0}; chunk_id_right < _right_in_table->chunk_count(); ++chunk_id_right) {
    const auto chunk_right = _right_in_table->get_chunk(chunk_id_right);
    const auto indices = chunk_right->get_indices(std::vector<ColumnID>{_right_column_id});
    if (track_right_matches) _right_matches[chunk_id_right].resize(chunk_right->size());

    std::shared_ptr<BaseIndex> index = nullptr;

    if (!indices.empty()) {
      // We assume the first index to be efficient for our join
      // as we do not want to spend time on evaluating the best index inside of this join loop
      index = indices.front();
    }

    // Scan all chunks from left input
    if (index != nullptr) {
      for (ChunkID chunk_id_left = ChunkID{0}; chunk_id_left < _left_in_table->chunk_count(); ++chunk_id_left) {
        const auto segment_left = _left_in_table->get_chunk(chunk_id_left)->get_segment(_left_column_id);

        resolve_data_and_segment_type(*segment_left, [&](auto left_type, auto& typed_left_segment) {
          using LeftType = typename decltype(left_type)::type;

          auto iterable_left = create_iterable_from_segment<LeftType>(typed_left_segment);

          // utilize index for join
          iterable_left.with_iterators([&](auto left_it, auto left_end) {
            _join_two_segments_using_index(left_it, left_end, chunk_id_left, chunk_id_right, index);
          });
        });
      }
      performance_data.chunks_scanned_with_index++;
    } else {
      // Fall back to NestedLoopJoin
      const auto segment_right = _right_in_table->get_chunk(chunk_id_right)->get_segment(_right_column_id);
      for (ChunkID chunk_id_left = ChunkID{0}; chunk_id_left < _left_in_table->chunk_count(); ++chunk_id_left) {
        const auto segment_left = _left_in_table->get_chunk(chunk_id_left)->get_segment(_left_column_id);
        JoinNestedLoop::JoinParams params{*_pos_list_left,
                                          *_pos_list_right,
                                          _left_matches[chunk_id_left],
                                          _right_matches[chunk_id_right],
                                          track_left_matches,
                                          track_right_matches,
                                          _mode,
                                          _predicate_condition};
        JoinNestedLoop::_join_two_untyped_segments(segment_left, segment_right, chunk_id_left, chunk_id_right, params);
      }
      performance_data.chunks_scanned_without_index++;
    }
  }

  // For Full Outer and Left Join we need to add all unmatched rows for the left side
  if (_mode == JoinMode::Left || _mode == JoinMode::Outer) {
    for (ChunkID chunk_id_left = ChunkID{0}; chunk_id_left < _left_in_table->chunk_count(); ++chunk_id_left) {
      for (ChunkOffset chunk_offset{0}; chunk_offset < _left_matches[chunk_id_left].size(); ++chunk_offset) {
        if (!_left_matches[chunk_id_left][chunk_offset]) {
          _pos_list_left->emplace_back(RowID{chunk_id_left, chunk_offset});
          _pos_list_right->emplace_back(NULL_ROW_ID);
        }
      }
    }
  }

  // For Full Outer and Right Join we need to add all unmatched rows for the right side.
  if (_mode == JoinMode::Outer || _mode == JoinMode::Right) {
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

  // write output chunks
  Segments output_segments;

  _write_output_segments(output_segments, _left_in_table, _pos_list_left);
  _write_output_segments(output_segments, _right_in_table, _pos_list_right);

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

    switch (_predicate_condition) {
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

// join loop that joins two segments of two columns via their iterators
template <typename BinaryFunctor, typename LeftIterator, typename RightIterator>
void JoinIndex::_join_two_segments_nested_loop(const BinaryFunctor& func, LeftIterator left_it, LeftIterator left_end,
                                               RightIterator right_begin, RightIterator right_end,
                                               const ChunkID chunk_id_left, const ChunkID chunk_id_right) {
  // No index so we fall back on a nested loop join
  for (; left_it != left_end; ++left_it) {
    const auto left_value = *left_it;
    if (left_value.is_null()) continue;

    for (auto right_it = right_begin; right_it != right_end; ++right_it) {
      const auto right_value = *right_it;
      if (right_value.is_null()) continue;

      if (func(left_value.value(), right_value.value())) {
        _pos_list_left->emplace_back(RowID{chunk_id_left, left_value.chunk_offset()});
        _pos_list_right->emplace_back(RowID{chunk_id_right, right_value.chunk_offset()});

        if (_mode == JoinMode::Left || _mode == JoinMode::Outer) {
          _left_matches[chunk_id_left][left_value.chunk_offset()] = true;
        }

        if (_mode == JoinMode::Outer || _mode == JoinMode::Right) {
          DebugAssert(chunk_id_right < _right_in_table->chunk_count(), "invalid chunk_id in join_index");
          DebugAssert(right_value.chunk_offset() < _right_in_table->get_chunk(chunk_id_right)->size(),
                      "invalid chunk_offset in join_index");
          _right_matches[chunk_id_right][right_value.chunk_offset()] = true;
        }
      }
    }
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
  if (_mode == JoinMode::Left || _mode == JoinMode::Outer) {
    _left_matches[chunk_id_left][chunk_offset_left] = true;
  }

  // we replicate the left value for each right value
  std::fill_n(std::back_inserter(*_pos_list_left), num_right_matches, RowID{chunk_id_left, chunk_offset_left});

  std::transform(range_begin, range_end, std::back_inserter(*_pos_list_right),
                 [chunk_id_right](ChunkOffset chunk_offset_right) {
                   return RowID{chunk_id_right, chunk_offset_right};
                 });

  if (_mode == JoinMode::Outer || _mode == JoinMode::Right) {
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
        for (const auto row : *pos_list) {
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
  _left_in_table.reset();
  _right_in_table.reset();
  _pos_list_left.reset();
  _pos_list_right.reset();
  _left_matches.clear();
  _right_matches.clear();
}

std::string JoinIndex::PerformanceData::to_string(DescriptionMode description_mode) const {
  std::string string = OperatorPerformanceData::to_string(description_mode);
  string += (description_mode == DescriptionMode::SingleLine ? " / " : "\\n");
  string += std::to_string(chunks_scanned_with_index) + " of " +
            std::to_string(chunks_scanned_with_index + chunks_scanned_without_index) + " chunks used an index";
  return string;
}

}  // namespace opossum
