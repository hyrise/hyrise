#include "join_nested_loop.hpp"

#include <memory>
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

void __attribute__((noinline))
process_match(RowID left_row_id, RowID right_row_id, const JoinNestedLoop::JoinParams& params) {
  // Write out a pair of matching row_ids - except for Semi/Anti joins, who build their output from params.left_matches
  // after all pairs were compared
  if (params.write_pos_lists) {
    params.pos_list_left.emplace_back(left_row_id);
    params.pos_list_right.emplace_back(right_row_id);
  }

  if (params.track_left_matches) {
    params.left_matches[left_row_id.chunk_offset] = true;
  }

  if (params.track_right_matches) {
    params.right_matches[right_row_id.chunk_offset] = true;
  }
}

// inner join loop that joins two segments via their iterators
// __attribute__((noinline)) to reduce compile time. As the hotloop is within this function, no performance
// loss expected
template <typename BinaryFunctor, typename LeftIterator, typename RightIterator>
void __attribute__((noinline))
join_two_typed_segments(const BinaryFunctor& func, LeftIterator left_it, LeftIterator left_end,
                        RightIterator right_begin, RightIterator right_end, const ChunkID chunk_id_left,
                        const ChunkID chunk_id_right, const JoinNestedLoop::JoinParams& params) {
  for (; left_it != left_end; ++left_it) {
    const auto left_value = *left_it;

    const auto left_row_id = RowID{chunk_id_left, left_value.chunk_offset()};

    for (auto right_it = right_begin; right_it != right_end; ++right_it) {
      const auto right_value = *right_it;
      const auto right_row_id = RowID{chunk_id_right, right_value.chunk_offset()};

      // AntiNullAsTrue is the only join mode where NULLs in any operand lead to a match. For all other
      // join modes, any NULL in the predicate results in a non-match.
      if (params.mode == JoinMode::AntiNullAsTrue) {
        if ((left_value.is_null() || right_value.is_null() || func(left_value.value(), right_value.value())) &&
            params.secondary_predicate_evaluator.satisfies_all_predicates(left_row_id, right_row_id)) {
          process_match(left_row_id, right_row_id, params);
        }
      } else {
        if ((!left_value.is_null() && !right_value.is_null() && func(left_value.value(), right_value.value())) &&
            params.secondary_predicate_evaluator.satisfies_all_predicates(left_row_id, right_row_id)) {
          process_match(left_row_id, right_row_id, params);
        }
      }
    }
  }
}
}  // namespace

namespace opossum {

bool JoinNestedLoop::supports(const JoinConfiguration config) { return true; }

JoinNestedLoop::JoinNestedLoop(const std::shared_ptr<const AbstractOperator>& left,
                               const std::shared_ptr<const AbstractOperator>& right, const JoinMode mode,
                               const OperatorJoinPredicate& primary_predicate,
                               const std::vector<OperatorJoinPredicate>& secondary_predicates)
    : AbstractJoinOperator(OperatorType::JoinNestedLoop, left, right, mode, primary_predicate, secondary_predicates) {
  // TODO(moritz) incorporate into supports()?
}

const std::string JoinNestedLoop::name() const { return "JoinNestedLoop"; }

std::shared_ptr<AbstractOperator> JoinNestedLoop::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<JoinNestedLoop>(copied_input_left, copied_input_right, _mode, _primary_predicate);
}

void JoinNestedLoop::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

std::shared_ptr<const Table> JoinNestedLoop::_on_execute() {
  Assert(supports({_mode, _primary_predicate.predicate_condition,
                   input_table_left()->column_data_type(_primary_predicate.column_ids.first),
                   input_table_right()->column_data_type(_primary_predicate.column_ids.second),
                   !_secondary_predicates.empty(), input_table_left()->type(), input_table_right()->type()}),
         "JoinNestedLoop doesn't support these parameters");

  PerformanceWarning("Nested Loop Join used");

  auto left_table = input_table_left();
  auto right_table = input_table_right();

  auto left_column_id = _primary_predicate.column_ids.first;
  auto right_column_id = _primary_predicate.column_ids.second;

  auto maybe_flipped_predicate_condition = _primary_predicate.predicate_condition;
  auto maybe_flipped_secondary_predicates = _secondary_predicates;

  if (_mode == JoinMode::Right) {
    // for Right Outer we swap the tables so we have the outer on the "left"
    std::swap(left_table, right_table);
    std::swap(left_column_id, right_column_id);
    maybe_flipped_predicate_condition = flip_predicate_condition(_primary_predicate.predicate_condition);

    for (auto& secondary_predicate : maybe_flipped_secondary_predicates) {
      secondary_predicate.flip();
    }
  }

  // Track pairs of matching RowIDs
  const auto pos_list_left = std::make_shared<PosList>();
  const auto pos_list_right = std::make_shared<PosList>();

  const auto is_outer_join = _mode == JoinMode::Left || _mode == JoinMode::Right || _mode == JoinMode::FullOuter;
  const auto is_semi_or_anti_join =
      _mode == JoinMode::Semi || _mode == JoinMode::AntiNullAsFalse || _mode == JoinMode::AntiNullAsTrue;

  const auto track_left_matches = is_outer_join || is_semi_or_anti_join;
  const auto track_right_matches = _mode == JoinMode::FullOuter;

  auto left_matches_by_chunk = std::vector<std::vector<bool>>(left_table->chunk_count());

  auto right_matches_by_chunk = std::vector<std::vector<bool>>(right_table->chunk_count());
  const auto chunk_count_right = right_table->chunk_count();
  for (ChunkID chunk_id_right = ChunkID{0}; chunk_id_right < chunk_count_right; ++chunk_id_right) {
    const auto chunk_right = right_table->get_chunk(chunk_id_right);
    Assert(chunk_right, "Did not expect deleted chunk here.");  // see #1686

    right_matches_by_chunk[chunk_id_right].resize(chunk_right->size());
  }

  auto secondary_predicate_evaluator =
      MultiPredicateJoinEvaluator{*left_table, *right_table, _mode, maybe_flipped_secondary_predicates};

  // Scan all chunks from left input
  const auto chunk_count_left = left_table->chunk_count();
  for (ChunkID chunk_id_left = ChunkID{0}; chunk_id_left < chunk_count_left; ++chunk_id_left) {
    const auto chunk_left = left_table->get_chunk(chunk_id_left);
    Assert(chunk_left, "Did not expect deleted chunk here.");  // see #1686

    auto segment_left = chunk_left->get_segment(left_column_id);

    std::vector<bool> left_matches;

    if (track_left_matches) {
      left_matches.resize(segment_left->size());
    }

    for (ChunkID chunk_id_right = ChunkID{0}; chunk_id_right < chunk_count_right; ++chunk_id_right) {
      const auto chunk_right = right_table->get_chunk(chunk_id_right);
      Assert(chunk_right, "Did not expect deleted chunk here.");  // see #1686

      const auto segment_right = chunk_right->get_segment(right_column_id);

      JoinParams params{*pos_list_left,
                        *pos_list_right,
                        left_matches,
                        right_matches_by_chunk[chunk_id_right],
                        track_left_matches,
                        track_right_matches,
                        _mode,
                        maybe_flipped_predicate_condition,
                        secondary_predicate_evaluator,
                        !is_semi_or_anti_join};
      _join_two_untyped_segments(*segment_left, *segment_right, chunk_id_left, chunk_id_right, params);
    }

    if (is_outer_join) {
      // Add unmatched rows on the left for Left and Full Outer joins
      for (ChunkOffset chunk_offset{0}; chunk_offset < left_matches.size(); ++chunk_offset) {
        if (!left_matches[chunk_offset]) {
          pos_list_left->emplace_back(RowID{chunk_id_left, chunk_offset});
          pos_list_right->emplace_back(NULL_ROW_ID);
        }
      }
    }

    left_matches_by_chunk[chunk_id_left] = std::move(left_matches);
  }

  // For Full Outer we need to add all unmatched rows for the right side.
  // Unmatched rows on the left side are already added in the main loop above
  if (_mode == JoinMode::FullOuter) {
    for (ChunkID chunk_id_right = ChunkID{0}; chunk_id_right < chunk_count_right; ++chunk_id_right) {
      const auto chunk_right = right_table->get_chunk(chunk_id_right);
      Assert(chunk_right, "Did not expect deleted chunk here.");  // see #1686

      const auto chunk_size = chunk_right->size();
      for (auto chunk_offset = ChunkOffset{0}; chunk_offset < chunk_size; ++chunk_offset) {
        if (!right_matches_by_chunk[chunk_id_right][chunk_offset]) {
          pos_list_left->emplace_back(NULL_ROW_ID);
          pos_list_right->emplace_back(RowID{chunk_id_right, chunk_offset});
        }
      }
    }
  }

  // Write PosLists for Semi/Anti Joins, which so far haven't written any results to the PosLists
  // We use `left_matches_by_chunk` to determine whether a tuple from the left side found a match.
  if (is_semi_or_anti_join) {
    const auto invert = _mode == JoinMode::AntiNullAsFalse || _mode == JoinMode::AntiNullAsTrue;

    for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count_left; ++chunk_id) {
      const auto chunk_left = left_table->get_chunk(chunk_id);
      Assert(chunk_left, "Did not expect deleted chunk here.");  // see #1686
      const auto chunk_size = chunk_left->size();
      for (auto chunk_offset = ChunkOffset{0}; chunk_offset < chunk_size; ++chunk_offset) {
        if (left_matches_by_chunk[chunk_id][chunk_offset] ^ invert) {
          pos_list_left->emplace_back(RowID{chunk_id, chunk_offset});
        }
      }
    }
  }

  // Write output Chunk based on the PosList(s) we created during the Join
  Segments segments;

  if (is_semi_or_anti_join) {
    _write_output_chunk(segments, left_table, pos_list_left);
  } else {
    if (_mode == JoinMode::Right) {
      _write_output_chunk(segments, right_table, pos_list_right);
      _write_output_chunk(segments, left_table, pos_list_left);
    } else {
      _write_output_chunk(segments, left_table, pos_list_left);
      _write_output_chunk(segments, right_table, pos_list_right);
    }
  }

  return _build_output_table({std::make_shared<Chunk>(std::move(segments))});
}

void JoinNestedLoop::_join_two_untyped_segments(const BaseSegment& base_segment_left,
                                                const BaseSegment& base_segment_right, const ChunkID chunk_id_left,
                                                const ChunkID chunk_id_right, JoinNestedLoop::JoinParams& params) {
  /**
   * This function dispatches `join_two_typed_segments()`.
   *
   * To reduce compile time, we erase the types of Segments and the PredicateCondition/comparator if
   * `base_segment_left.data_type() != base_segment_left.data_type()` or `LeftSegmentType != RightSegmentType`. This is
   * the "SLOW PATH".
   * If data types and segment types are the same, we take the "FAST PATH", where only the SegmentType of left segment
   * is erased and inlining optimization can be performed by the compiler for the inner loop.
   *
   * Having this SLOW PATH and erasing the SegmentType even for the FAST PATH are essential for keeping the compile time
   * of the JoinNestedLoop reasonably low.
   */

  /**
   * FAST PATH
   */
  if (base_segment_left.data_type() == base_segment_right.data_type()) {
    auto fast_path_taken = false;

    resolve_data_and_segment_type(base_segment_left, [&](const auto data_type_t, const auto& segment_left) {
      using ColumnDataType = typename decltype(data_type_t)::type;
      using LeftSegmentType = std::decay_t<decltype(segment_left)>;

      if (const auto* segment_right = dynamic_cast<const LeftSegmentType*>(&base_segment_right)) {
        const auto iterable_left = create_any_segment_iterable<ColumnDataType>(segment_left);
        const auto iterable_right = create_iterable_from_segment<ColumnDataType>(*segment_right);

        iterable_left.with_iterators([&](auto left_begin, const auto& left_end) {
          iterable_right.with_iterators([&](auto right_begin, const auto& right_end) {
            with_comparator(params.predicate_condition, [&](auto comparator) {
              join_two_typed_segments(comparator, left_begin, left_end, right_begin, right_end, chunk_id_left,
                                      chunk_id_right, params);
            });
          });
        });

        fast_path_taken = true;
      }
    });

    if (fast_path_taken) {
      return;
    }
  }

  /**
   * SLOW PATH
   */
  // clang-format off
  segment_with_iterators<ResolveDataTypeTag, EraseTypes::Always>(base_segment_left, [&](auto left_it, const auto left_end) {  // NOLINT
    segment_with_iterators<ResolveDataTypeTag, EraseTypes::Always>(base_segment_right, [&](auto right_it, const auto right_end) {  // NOLINT
      using LeftType = typename std::decay_t<decltype(left_it)>::ValueType;
      using RightType = typename std::decay_t<decltype(right_it)>::ValueType;

      // make sure that we do not compile invalid versions of these lambdas
      constexpr auto LEFT_IS_STRING_COLUMN = (std::is_same<LeftType, pmr_string>{});
      constexpr auto RIGHT_IS_STRING_COLUMN = (std::is_same<RightType, pmr_string>{});

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

        // Erase the `predicate_condition` into a std::function<>
        auto erased_comparator = std::function<bool(const LeftType&, const RightType&)>{};
        with_comparator(params_copy.predicate_condition, [&](auto comparator) { erased_comparator = comparator; });

        join_two_typed_segments(erased_comparator, left_it_copy, left_end_copy, right_it_copy, right_end_copy,
                                       chunk_id_left_copy, chunk_id_right_copy, params_copy);
      } else {
        // gcc complains without these
        ignore_unused_variable(right_end);
        ignore_unused_variable(left_end);

        Fail("Cannot join String with non-String column");
      }
    });
  });
  // clang-format on
}

void JoinNestedLoop::_write_output_chunk(Segments& segments, const std::shared_ptr<const Table>& input_table,
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

}  // namespace opossum
