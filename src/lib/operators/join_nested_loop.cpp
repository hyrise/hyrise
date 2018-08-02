#include "join_nested_loop.hpp"

#include <map>
#include <memory>
#include <numeric>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "resolve_type.hpp"
#include "storage/column_iterables/any_column_iterable.hpp"
#include "storage/create_iterable_from_column.hpp"
#include "type_comparison.hpp"
#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"

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

  _create_table_structure();

  _perform_join();

  return _output_table;
}

void JoinNestedLoop::_create_table_structure() {
  _left_in_table = _input_left->get_output();
  _right_in_table = _input_right->get_output();

  _left_column_id = _column_ids.first;
  _right_column_id = _column_ids.second;

  const bool left_may_produce_null = (_mode == JoinMode::Right || _mode == JoinMode::Outer);
  const bool right_may_produce_null = (_mode == JoinMode::Left || _mode == JoinMode::Outer);

  TableColumnDefinitions output_column_definitions;

  // Preparing output table by adding columns from left table
  for (ColumnID column_id{0}; column_id < _left_in_table->column_count(); ++column_id) {
    const auto nullable = (left_may_produce_null || _left_in_table->column_is_nullable(column_id));
    output_column_definitions.emplace_back(_left_in_table->column_name(column_id),
                                           _left_in_table->column_data_type(column_id), nullable);
  }

  // Preparing output table by adding columns from right table
  for (ColumnID column_id{0}; column_id < _right_in_table->column_count(); ++column_id) {
    const auto nullable = (right_may_produce_null || _right_in_table->column_is_nullable(column_id));
    output_column_definitions.emplace_back(_right_in_table->column_name(column_id),
                                           _right_in_table->column_data_type(column_id), nullable);
  }

  _output_table = std::make_shared<Table>(output_column_definitions, TableType::References);
}

void JoinNestedLoop::_process_match(RowID left_row_id, RowID right_row_id, JoinNestedLoop::JoinParams& params) {
  params.pos_list_left.emplace_back(left_row_id);
  params.pos_list_right.emplace_back(right_row_id);

  if (params.track_left_matches) {
    params.left_matches[left_row_id.chunk_offset] = true;
  }

  if (params.track_right_matches) {
    params.right_matches[right_row_id.chunk_offset] = true;
  }
}

// inner join loop that joins two columns via their iterators
template <typename BinaryFunctor, typename LeftIterator, typename RightIterator>
void JoinNestedLoop::_join_two_typed_columns(const BinaryFunctor& func, LeftIterator left_it, LeftIterator left_end,
                                             RightIterator right_begin, RightIterator right_end,
                                             const ChunkID chunk_id_left, const ChunkID chunk_id_right,
                                             JoinNestedLoop::JoinParams& params) {
  for (; left_it != left_end; ++left_it) {
    const auto left_value = *left_it;
    if (left_value.is_null()) continue;

    for (auto right_it = right_begin; right_it != right_end; ++right_it) {
      const auto right_value = *right_it;
      if (right_value.is_null()) continue;

      if (func(left_value.value(), right_value.value())) {
        _process_match(RowID{chunk_id_left, left_value.chunk_offset()},
                       RowID{chunk_id_right, right_value.chunk_offset()}, params);
      }
    }
  }
}

void JoinNestedLoop::_join_two_untyped_columns(const std::shared_ptr<const BaseColumn>& column_left,
                                               const std::shared_ptr<const BaseColumn>& column_right,
                                               const ChunkID chunk_id_left, const ChunkID chunk_id_right,
                                               JoinNestedLoop::JoinParams& params) {
  resolve_data_and_column_type(*column_left, [&](auto left_type, auto& typed_left_column) {
    resolve_data_and_column_type(*column_right, [&](auto right_type, auto& typed_right_column) {
      using LeftType = typename decltype(left_type)::type;
      using RightType = typename decltype(right_type)::type;

      // make sure that we do not compile invalid versions of these lambdas
      constexpr auto LEFT_IS_STRING_COLUMN = (std::is_same<LeftType, std::string>{});
      constexpr auto RIGHT_IS_STRING_COLUMN = (std::is_same<RightType, std::string>{});

      constexpr auto NEITHER_IS_STRING_COLUMN = !LEFT_IS_STRING_COLUMN && !RIGHT_IS_STRING_COLUMN;
      constexpr auto BOTH_ARE_STRING_COLUMNS = LEFT_IS_STRING_COLUMN && RIGHT_IS_STRING_COLUMN;

      // clang-format off
      if constexpr (NEITHER_IS_STRING_COLUMN || BOTH_ARE_STRING_COLUMNS) {
        auto iterable_left = create_iterable_from_column<LeftType>(typed_left_column);
        auto iterable_right = create_iterable_from_column<RightType>(typed_right_column);

        iterable_left.with_iterators([&](auto left_it, auto left_end) {
          iterable_right.with_iterators([&](auto right_it, auto right_end) {
            with_comparator(params.predicate_condition, [&](auto comparator) {
              _join_two_typed_columns(comparator, left_it, left_end, right_it, right_end, chunk_id_left,
                                      chunk_id_right, params);
            });
          });
        });
      }
      // clang-format on
    });
  });
}

void JoinNestedLoop::_perform_join() {
  auto left_table = _left_in_table;
  auto right_table = _right_in_table;

  auto left_column_id = _left_column_id;
  auto right_column_id = _right_column_id;

  if (_mode == JoinMode::Right) {
    // for Right Outer we swap the tables so we have the outer on the "left"
    left_table = _right_in_table;
    right_table = _left_in_table;

    left_column_id = _right_column_id;
    right_column_id = _left_column_id;
  }

  _pos_list_left = std::make_shared<PosList>();
  _pos_list_right = std::make_shared<PosList>();

  _is_outer_join = (_mode == JoinMode::Left || _mode == JoinMode::Right || _mode == JoinMode::Outer);

  // Scan all chunks from left input
  _right_matches.resize(right_table->chunk_count());
  for (ChunkID chunk_id_left = ChunkID{0}; chunk_id_left < left_table->chunk_count(); ++chunk_id_left) {
    auto column_left = left_table->get_chunk(chunk_id_left)->get_column(left_column_id);

    // for Outer joins, remember matches on the left side
    std::vector<bool> left_matches;

    if (_is_outer_join) {
      left_matches.resize(column_left->size());
    }

    // Scan all chunks for right input
    for (ChunkID chunk_id_right = ChunkID{0}; chunk_id_right < right_table->chunk_count(); ++chunk_id_right) {
      const auto column_right = right_table->get_chunk(chunk_id_right)->get_column(right_column_id);
      _right_matches[chunk_id_right].resize(column_right->size());

      const auto track_right_matches = (_mode == JoinMode::Outer);
      JoinParams params{*_pos_list_left, *_pos_list_right,    left_matches, _right_matches[chunk_id_right],
                        _is_outer_join,  track_right_matches, _mode,        _predicate_condition};
      _join_two_untyped_columns(column_left, column_right, chunk_id_left, chunk_id_right, params);
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
      const auto column_right = right_table->get_chunk(chunk_id_right)->get_column(right_column_id);

      resolve_data_and_column_type(*column_right, [&](auto right_type, auto& typed_right_column) {
        using RightType = typename decltype(right_type)::type;

        auto iterable_right = create_iterable_from_column<RightType>(typed_right_column);

        iterable_right.for_each([&](const auto& right_value) {
          const auto row_id = RowID{chunk_id_right, right_value.chunk_offset()};
          if (!_right_matches[chunk_id_right][row_id.chunk_offset]) {
            _pos_list_left->emplace_back(NULL_ROW_ID);
            _pos_list_right->emplace_back(row_id);
          }
        });
      });
    }
  }

  // write output chunks
  ChunkColumns columns;

  if (_mode == JoinMode::Right) {
    _write_output_chunks(columns, right_table, _pos_list_right);
    _write_output_chunks(columns, left_table, _pos_list_left);
  } else {
    _write_output_chunks(columns, left_table, _pos_list_left);
    _write_output_chunks(columns, right_table, _pos_list_right);
  }

  _output_table->append_chunk(columns);
}

void JoinNestedLoop::_write_output_chunks(ChunkColumns& columns, const std::shared_ptr<const Table>& input_table,
                                          const std::shared_ptr<PosList>& pos_list) {
  // Add columns from table to output chunk
  for (ColumnID column_id{0}; column_id < input_table->column_count(); ++column_id) {
    std::shared_ptr<BaseColumn> column;

    if (input_table->type() == TableType::References) {
      if (input_table->chunk_count() > 0) {
        auto new_pos_list = std::make_shared<PosList>();

        // de-reference to the correct RowID so the output can be used in a Multi Join
        for (const auto row : *pos_list) {
          if (row.is_null()) {
            new_pos_list->push_back(NULL_ROW_ID);
          } else {
            auto reference_column = std::static_pointer_cast<const ReferenceColumn>(
                input_table->get_chunk(row.chunk_id)->get_column(column_id));
            new_pos_list->push_back(reference_column->pos_list()->at(row.chunk_offset));
          }
        }

        auto reference_column =
            std::static_pointer_cast<const ReferenceColumn>(input_table->get_chunk(ChunkID{0})->get_column(column_id));

        column = std::make_shared<ReferenceColumn>(reference_column->referenced_table(),
                                                   reference_column->referenced_column_id(), new_pos_list);
      } else {
        // If there are no Chunks in the input_table, we can't deduce the Table that input_table is referencING to
        // pos_list will contain only NULL_ROW_IDs anyway, so it doesn't matter which Table the ReferenceColumn that
        // we output is referencing. HACK, but works fine: we create a dummy table and let the ReferenceColumn ref
        // it.
        const auto dummy_table = Table::create_dummy_table(input_table->column_definitions());
        column = std::make_shared<ReferenceColumn>(dummy_table, column_id, pos_list);
      }
    } else {
      column = std::make_shared<ReferenceColumn>(input_table, column_id, pos_list);
    }

    columns.push_back(column);
  }
}

void JoinNestedLoop::_on_cleanup() {
  _output_table.reset();
  _left_in_table.reset();
  _right_in_table.reset();
  _pos_list_left.reset();
  _pos_list_right.reset();
  _right_matches.clear();
}

}  // namespace opossum
