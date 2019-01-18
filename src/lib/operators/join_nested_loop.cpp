#include "join_nested_loop.hpp"

#include <map>
#include <memory>
#include <numeric>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "resolve_type.hpp"
#include "storage/create_iterable_from_segment.hpp"
#include "storage/segment_iterables/any_segment_iterable.hpp"
#include "storage/segment_iterate.hpp"
#include "type_comparison.hpp"
#include "utils/assert.hpp"
#include "utils/ignore_unused_variable.hpp"
#include "utils/performance_warning.hpp"

namespace {
using namespace opossum;  // NOLINT
void process_match(RowID left_row_id, RowID right_row_id, const JoinNestedLoop::JoinParams& params) {
  params.pos_list_left.emplace_back(left_row_id);
  params.pos_list_right.emplace_back(right_row_id);

  if (params.track_left_matches) {
    params.left_matches[left_row_id.chunk_offset] = true;
  }

  if (params.track_right_matches) {
    params.right_matches[right_row_id.chunk_offset] = true;
  }
}

// inner join loop that joins two segments via their iterators
template <typename BinaryFunctor, typename LeftIterator, typename RightIterator>
void join_two_typed_segments(const BinaryFunctor& func, LeftIterator left_it, LeftIterator left_end,
                             RightIterator right_begin, RightIterator right_end, const ChunkID chunk_id_left,
                             const ChunkID chunk_id_right, const JoinNestedLoop::JoinParams& params) {
  for (; left_it != left_end; ++left_it) {
    const auto left_value = *left_it;
    if (left_value.is_null()) continue;

    for (auto right_it = right_begin; right_it != right_end; ++right_it) {
      const auto right_value = *right_it;
      if (right_value.is_null()) continue;

      if (func(left_value.value(), right_value.value())) {
        process_match(RowID{chunk_id_left, left_value.chunk_offset()},
                      RowID{chunk_id_right, right_value.chunk_offset()}, params);
      }
    }
  }
}
}  // namespace

namespace opossum {

/*
 * This is a Nested Loop Join implementation completely based on iterables.
 * It supports all current join and predicate conditions, as well as NULL values.
 * Because this is a Nested Loop Join, the performance is going to be far inferior to JoinHash and JoinSortMerge,
 * so only use this for testing or benchmarking purposes.
 */

JoinNestedLoop::JoinNestedLoop(const std::shared_ptr<const AbstractOperator>& left,
                               const std::shared_ptr<const AbstractOperator>& right, const JoinMode mode,
                               const ColumnIDPair& column_ids, const PredicateCondition predicate_condition)
    : AbstractJoinOperator(OperatorType::JoinNestedLoop, left, right, mode, column_ids, predicate_condition) {}

const std::string JoinNestedLoop::name() const { return "JoinNestedLoop"; }

std::shared_ptr<AbstractOperator> JoinNestedLoop::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<JoinNestedLoop>(copied_input_left, copied_input_right, _mode, _column_ids,
                                          _predicate_condition);
}

void JoinNestedLoop::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

std::shared_ptr<const Table> JoinNestedLoop::_on_execute() {
  PerformanceWarning("Nested Loop Join used");

  _output_table = _initialize_output_table();

  _perform_join();

  return _output_table;
}

void JoinNestedLoop::_join_two_untyped_segments(const std::shared_ptr<const BaseSegment>& segment_left,
                                                const std::shared_ptr<const BaseSegment>& segment_right,
                                                const ChunkID chunk_id_left, const ChunkID chunk_id_right,
                                                JoinNestedLoop::JoinParams& params) {
  /**
   * The nested loops.
   *
   * The value in the outer loop is retrieved via virtual function calls ("EraseTypes::Always") and only the inner loop
   * gets inlined. This is to keep the compile time of the JoinNestedLoop *somewhat* at bay, if we inline both the inner
   * and the outer loop, the JoinNestedLoop becomes the most expensive-to-compile file in all of Hyrise by a margin
   */
  segment_with_iterators<ResolveDataTypeTag, EraseTypes::Always>(*segment_left, [&](auto left_it, const auto left_end) {
    segment_with_iterators(*segment_right, [&](auto right_it, const auto right_end) {
      using LeftType = typename decltype(left_it)::ValueType;
      using RightType = typename decltype(right_it)::ValueType;

      // make sure that we do not compile invalid versions of these lambdas
      constexpr auto LEFT_IS_STRING_COLUMN = (std::is_same<LeftType, std::string>{});
      constexpr auto RIGHT_IS_STRING_COLUMN = (std::is_same<RightType, std::string>{});

      constexpr auto NEITHER_IS_STRING_COLUMN = !LEFT_IS_STRING_COLUMN && !RIGHT_IS_STRING_COLUMN;
      constexpr auto BOTH_ARE_STRING_COLUMN = LEFT_IS_STRING_COLUMN && RIGHT_IS_STRING_COLUMN;

      if constexpr (NEITHER_IS_STRING_COLUMN || BOTH_ARE_STRING_COLUMN) {
        // Dirty hack to avoid https://gcc.gnu.org/bugzilla/show_bug.cgi?id=86740
        const auto left_it_copy = left_it;
        const auto left_end_copy = left_end;
        const auto right_it_copy = right_it;
        const auto right_end_copy = right_end;
        const auto params_copy = params;
        const auto chunk_id_left_copy = chunk_id_left;
        const auto chunk_id_right_copy = chunk_id_right;

        with_comparator(params_copy.predicate_condition, [&](auto comparator) {
          join_two_typed_segments(comparator, left_it_copy, left_end_copy, right_it_copy, right_end_copy,
                                  chunk_id_left_copy, chunk_id_right_copy, params_copy);
        });
      } else {
        // gcc complains without these
        ignore_unused_variable(right_end);
        ignore_unused_variable(left_end);

        Fail("Cannot join String with non-String column");
      }
    });
  });
}

void JoinNestedLoop::_perform_join() {
  auto left_table = input_table_left();
  auto right_table = input_table_right();

  auto left_column_id = _column_ids.first;
  auto right_column_id = _column_ids.second;

  if (_mode == JoinMode::Right) {
    // for Right Outer we swap the tables so we have the outer on the "left"
    std::swap(left_table, right_table);
    std::swap(left_column_id, right_column_id);
  }

  _pos_list_left = std::make_shared<PosList>();
  _pos_list_right = std::make_shared<PosList>();

  _is_outer_join = (_mode == JoinMode::Left || _mode == JoinMode::Right || _mode == JoinMode::Outer);

  // Scan all chunks from left input
  _right_matches.resize(right_table->chunk_count());
  for (ChunkID chunk_id_left = ChunkID{0}; chunk_id_left < left_table->chunk_count(); ++chunk_id_left) {
    auto segment_left = left_table->get_chunk(chunk_id_left)->get_segment(left_column_id);

    // for Outer joins, remember matches on the left side
    std::vector<bool> left_matches;

    if (_is_outer_join) {
      left_matches.resize(segment_left->size());
    }

    // Scan all chunks for right input
    for (ChunkID chunk_id_right = ChunkID{0}; chunk_id_right < right_table->chunk_count(); ++chunk_id_right) {
      const auto segment_right = right_table->get_chunk(chunk_id_right)->get_segment(right_column_id);
      _right_matches[chunk_id_right].resize(segment_right->size());

      const auto track_right_matches = (_mode == JoinMode::Outer);
      JoinParams params{*_pos_list_left, *_pos_list_right,    left_matches, _right_matches[chunk_id_right],
                        _is_outer_join,  track_right_matches, _mode,        _predicate_condition};
      _join_two_untyped_segments(segment_left, segment_right, chunk_id_left, chunk_id_right, params);
    }

    if (_is_outer_join) {
      // add unmatched rows on the left for Left and Full Outer joins
      for (ChunkOffset chunk_offset{0}; chunk_offset < left_matches.size(); ++chunk_offset) {
        if (!left_matches[chunk_offset]) {
          _pos_list_left->emplace_back(RowID{chunk_id_left, chunk_offset});
          _pos_list_right->emplace_back(NULL_ROW_ID);
        }
      }
    }
  }

  // For Full Outer we need to add all unmatched rows for the right side.
  // Unmatched rows on the left side are already added in the main loop above
  if (_mode == JoinMode::Outer) {
    for (ChunkID chunk_id_right = ChunkID{0}; chunk_id_right < right_table->chunk_count(); ++chunk_id_right) {
      const auto segment_right = right_table->get_chunk(chunk_id_right)->get_segment(right_column_id);

      segment_iterate(*segment_right, [&](const auto& position) {
        const auto row_id = RowID{chunk_id_right, position.chunk_offset()};
        if (!_right_matches[chunk_id_right][row_id.chunk_offset]) {
          _pos_list_left->emplace_back(NULL_ROW_ID);
          _pos_list_right->emplace_back(row_id);
        }
      });
    }
  }

  // write output chunks
  Segments segments;

  if (_mode == JoinMode::Right) {
    _write_output_chunks(segments, right_table, _pos_list_right);
    _write_output_chunks(segments, left_table, _pos_list_left);
  } else {
    _write_output_chunks(segments, left_table, _pos_list_left);
    _write_output_chunks(segments, right_table, _pos_list_right);
  }

  _output_table->append_chunk(segments);
}

void JoinNestedLoop::_write_output_chunks(Segments& segments, const std::shared_ptr<const Table>& input_table,
                                          const std::shared_ptr<PosList>& pos_list) {
  // Add segments from table to output chunk
  for (ColumnID column_id{0}; column_id < input_table->column_count(); ++column_id) {
    std::shared_ptr<BaseSegment> segment;

    if (input_table->type() == TableType::References) {
      if (input_table->chunk_count() > 0) {
        auto new_pos_list = std::make_shared<PosList>();

        // de-reference to the correct RowID so the output can be used in a Multi Join
        for (const auto& row : *pos_list) {
          if (row.is_null()) {
            new_pos_list->push_back(NULL_ROW_ID);
          } else {
            auto reference_segment = std::static_pointer_cast<const ReferenceSegment>(
                input_table->get_chunk(row.chunk_id)->get_segment(column_id));
            new_pos_list->push_back((*reference_segment->pos_list())[row.chunk_offset]);
          }
        }

        auto reference_segment = std::static_pointer_cast<const ReferenceSegment>(
            input_table->get_chunk(ChunkID{0})->get_segment(column_id));

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

    segments.push_back(segment);
  }
}

void JoinNestedLoop::_on_cleanup() {
  _output_table.reset();
  _pos_list_left.reset();
  _pos_list_right.reset();
  _right_matches.clear();
}

}  // namespace opossum
