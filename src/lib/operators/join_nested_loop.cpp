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

JoinNestedLoop::JoinNestedLoop(const std::shared_ptr<const AbstractOperator> left,
                               const std::shared_ptr<const AbstractOperator> right, const JoinMode mode,
                               const ColumnIDPair& column_ids, const PredicateCondition predicate_condition)
    : AbstractJoinOperator(left, right, mode, column_ids, predicate_condition) {}

const std::string JoinNestedLoop::name() const { return "JoinNestedLoop"; }

std::shared_ptr<AbstractOperator> JoinNestedLoop::recreate(const std::vector<AllParameterVariant>& args) const {
  return std::make_shared<JoinNestedLoop>(_input_left->recreate(args), _input_right->recreate(args), _mode, _column_ids,
                                          _predicate_condition);
}

std::shared_ptr<const Table> JoinNestedLoop::_on_execute() {
  PerformanceWarning("Nested Loop Join used");

  _create_table_structure();

  _perform_join();

  return _output_table;
}

void JoinNestedLoop::_create_table_structure() {
  _output_table = std::make_shared<Table>();

  _left_in_table = _input_left->get_output();
  _right_in_table = _input_right->get_output();

  _left_column_id = _column_ids.first;
  _right_column_id = _column_ids.second;

  const bool left_may_produce_null = (_mode == JoinMode::Right || _mode == JoinMode::Outer);
  const bool right_may_produce_null = (_mode == JoinMode::Left || _mode == JoinMode::Outer);

  // Preparing output table by adding columns from left table
  for (ColumnID column_id{0}; column_id < _left_in_table->column_count(); ++column_id) {
    auto nullable = (left_may_produce_null || _left_in_table->column_is_nullable(column_id));
    _output_table->add_column_definition(_left_in_table->column_name(column_id), _left_in_table->column_type(column_id),
                                         nullable);
  }

  // Preparing output table by adding columns from right table
  for (ColumnID column_id{0}; column_id < _right_in_table->column_count(); ++column_id) {
    auto nullable = (right_may_produce_null || _right_in_table->column_is_nullable(column_id));
    _output_table->add_column_definition(_right_in_table->column_name(column_id),
                                         _right_in_table->column_type(column_id), nullable);
  }
}

// inner join loop that joins two columns via their iterators
template <typename BinaryFunctor, typename LeftIterator, typename RightIterator>
void JoinNestedLoop::_join_two_columns(const BinaryFunctor& func, LeftIterator left_it, LeftIterator left_end,
                                       RightIterator right_begin, RightIterator right_end, const ChunkID chunk_id_left,
                                       const ChunkID chunk_id_right, std::vector<bool>& left_matches) {
  for (; left_it != left_end; ++left_it) {
    const auto left_value = *left_it;
    if (left_value.is_null()) continue;

    for (auto right_it = right_begin; right_it != right_end; ++right_it) {
      const auto right_value = *right_it;
      if (right_value.is_null()) continue;

      if (func(left_value.value(), right_value.value())) {
        _pos_list_left->emplace_back(RowID{chunk_id_left, left_value.chunk_offset()});
        _pos_list_right->emplace_back(RowID{chunk_id_right, right_value.chunk_offset()});

        if (_is_outer_join) {
          left_matches[left_value.chunk_offset()] = true;
        }

        if (_mode == JoinMode::Outer) {
          _right_matches.insert(RowID{chunk_id_right, right_value.chunk_offset()});
        }
      }
    }
  }
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

  auto left_data_type = left_table->column_type(left_column_id);
  auto right_data_type = right_table->column_type(right_column_id);

  _pos_list_left = std::make_shared<PosList>();
  _pos_list_right = std::make_shared<PosList>();

  _is_outer_join = (_mode == JoinMode::Left || _mode == JoinMode::Right || _mode == JoinMode::Outer);

  // Scan all chunks from left input
  for (ChunkID chunk_id_left = ChunkID{0}; chunk_id_left < left_table->chunk_count(); ++chunk_id_left) {
    auto column_left = left_table->get_chunk(chunk_id_left)->get_column(left_column_id);

    // for Outer joins, remember matches on the left side
    std::vector<bool> left_matches;

    if (_is_outer_join) {
      left_matches.resize(column_left->size());
    }

    // Scan all chunks for right input
    for (ChunkID chunk_id_right = ChunkID{0}; chunk_id_right < right_table->chunk_count(); ++chunk_id_right) {
      auto column_right = right_table->get_chunk(chunk_id_right)->get_column(right_column_id);

      resolve_data_and_column_type(left_data_type, *column_left, [&](auto left_type, auto& typed_left_column) {
        resolve_data_and_column_type(right_data_type, *column_right, [&](auto right_type, auto& typed_right_column) {
          using LeftType = typename decltype(left_type)::type;
          using RightType = typename decltype(right_type)::type;

          // make sure that we do not compile invalid versions of these lambdas
          constexpr auto left_is_string_column = (std::is_same<LeftType, std::string>{});
          constexpr auto right_is_string_column = (std::is_same<RightType, std::string>{});

          constexpr auto neither_is_string_column = !left_is_string_column && !right_is_string_column;
          constexpr auto both_are_string_columns = left_is_string_column && right_is_string_column;

          // clang-format off
          if constexpr (neither_is_string_column || both_are_string_columns) {
            auto iterable_left = erase_type_from_iterable(create_iterable_from_column<LeftType>(typed_left_column));
            auto iterable_right = create_iterable_from_column<RightType>(typed_right_column);

            iterable_left.require_single_functor_call();
            iterable_right.require_single_functor_call();

            iterable_left.with_iterators([&](auto left_it, auto left_end) {
              iterable_right.with_iterators([&](auto right_it, auto right_end) {
                with_comparator(_predicate_condition, [&](auto comparator) {
                  this->_join_two_columns(comparator, left_it, left_end, right_it, right_end, chunk_id_left,
                                          chunk_id_right, left_matches);
                });
              });
            });
          }
          // clang-format on
        });
      });
    }

    if (_is_outer_join) {
      // add unmatched rows on the left for Left and Full Outer joins
      for (ChunkOffset chunk_offset{0}; chunk_offset < left_matches.size(); ++chunk_offset) {
        if (!left_matches[chunk_offset]) {
          _pos_list_left->emplace_back(RowID{chunk_id_left, chunk_offset});
          _pos_list_right->emplace_back(RowID{ChunkID{0}, INVALID_CHUNK_OFFSET});
        }
      }
    }
  }

  // For Full Outer we need to add all unmatched rows for the right side.
  // Unmatched rows on the left side are already added in the main loop above
  if (_mode == JoinMode::Outer) {
    for (ChunkID chunk_id_right = ChunkID{0}; chunk_id_right < right_table->chunk_count(); ++chunk_id_right) {
      auto column_right = right_table->get_chunk(chunk_id_right)->get_column(right_column_id);

      resolve_data_and_column_type(right_data_type, *column_right, [&](auto right_type, auto& typed_right_column) {
        using RightType = typename decltype(right_type)::type;

        auto iterable_right = create_iterable_from_column<RightType>(typed_right_column);

        iterable_right.for_each([&](const auto& right_value) {
          const auto row_id = RowID{chunk_id_right, right_value.chunk_offset()};
          if (!_right_matches.count(row_id)) {
            _pos_list_left->emplace_back(RowID{ChunkID{0}, INVALID_CHUNK_OFFSET});
            _pos_list_right->emplace_back(row_id);
          }
        });
      });
    }
  }

  // write output chunks
  auto output_chunk = std::make_shared<Chunk>();

  if (_mode == JoinMode::Right) {
    _write_output_chunks(output_chunk, right_table, _pos_list_right);
    _write_output_chunks(output_chunk, left_table, _pos_list_left);
  } else {
    _write_output_chunks(output_chunk, left_table, _pos_list_left);
    _write_output_chunks(output_chunk, right_table, _pos_list_right);
  }

  _output_table->emplace_chunk(std::move(output_chunk));
}

void JoinNestedLoop::_write_output_chunks(const std::shared_ptr<Chunk>& output_chunk,
                                          const std::shared_ptr<const Table> input_table,
                                          std::shared_ptr<PosList> pos_list) {
  // Add columns from table to output chunk
  for (ColumnID column_id{0}; column_id < input_table->column_count(); ++column_id) {
    std::shared_ptr<BaseColumn> column;

    if (auto reference_column = std::dynamic_pointer_cast<const ReferenceColumn>(
            input_table->get_chunk(ChunkID{0})->get_column(column_id))) {
      auto new_pos_list = std::make_shared<PosList>();

      ChunkID current_chunk_id{0};

      // de-reference to the correct RowID so the output can be used in a Multi Join
      for (const auto row : *pos_list) {
        if (row.chunk_id != current_chunk_id) {
          current_chunk_id = row.chunk_id;

          reference_column = std::dynamic_pointer_cast<const ReferenceColumn>(
              input_table->get_chunk(current_chunk_id)->get_column(column_id));
        }
        if (row.chunk_offset == INVALID_CHUNK_OFFSET) {
          new_pos_list->push_back(RowID{ChunkID{0}, INVALID_CHUNK_OFFSET});
        } else {
          new_pos_list->push_back(reference_column->pos_list()->at(row.chunk_offset));
        }
      }

      column = std::make_shared<ReferenceColumn>(reference_column->referenced_table(),
                                                 reference_column->referenced_column_id(), new_pos_list);
    } else {
      column = std::make_shared<ReferenceColumn>(input_table, column_id, pos_list);
    }

    output_chunk->add_column(column);
  }
}

}  // namespace opossum
