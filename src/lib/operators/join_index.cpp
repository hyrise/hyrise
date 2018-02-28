#include "join_index.hpp"

#include <map>
#include <memory>
#include <numeric>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "all_type_variant.hpp"
#include "resolve_type.hpp"
#include "storage/create_iterable_from_column.hpp"
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

JoinIndex::JoinIndex(const std::shared_ptr<const AbstractOperator> left,
                     const std::shared_ptr<const AbstractOperator> right, const JoinMode mode,
                     const std::pair<ColumnID, ColumnID>& column_ids, const PredicateCondition predicate_condition)
    : AbstractJoinOperator(left, right, mode, column_ids, predicate_condition) {
  DebugAssert(mode != JoinMode::Cross, "Cross Join is not supported by index join.");
}

const std::string JoinIndex::name() const { return "JoinIndex"; }

std::shared_ptr<AbstractOperator> JoinIndex::recreate(const std::vector<AllParameterVariant>& args) const {
  return std::make_shared<JoinIndex>(_input_left->recreate(args), _input_right->recreate(args), _mode, _column_ids,
                                     _predicate_condition);
}

std::shared_ptr<const Table> JoinIndex::_on_execute() {
  _create_table_structure();

  _perform_join();

  return _output_table;
}

void JoinIndex::_create_table_structure() {
  _output_table = std::make_shared<Table>();

  _left_in_table = _input_left->get_output();
  _right_in_table = _input_right->get_output();

  _left_column_id = _column_ids.first;
  _right_column_id = _column_ids.second;

  const bool left_may_produce_null = (_mode == JoinMode::Right || _mode == JoinMode::Outer);
  const bool right_may_produce_null = (_mode == JoinMode::Left || _mode == JoinMode::Outer);

  // Preparing output table by adding columns from left and right table
  auto add_column_definitions = [&](auto from_table, bool from_may_produce_null) {
    for (ColumnID column_id{0}; column_id < from_table->column_count(); ++column_id) {
      auto nullable = (from_may_produce_null || from_table->column_is_nullable(column_id));
      _output_table->add_column_definition(from_table->column_name(column_id), from_table->column_type(column_id),
                                           nullable);
    }
  };

  add_column_definitions(_left_in_table, left_may_produce_null);
  add_column_definitions(_right_in_table, right_may_produce_null);
}

void JoinIndex::_perform_join() {
  _right_matches.resize(_right_in_table->chunk_count());
  _left_matches.resize(_left_in_table->chunk_count());

  if (_mode == JoinMode::Left || _mode == JoinMode::Outer) {
    for (ChunkID chunk_id_left = ChunkID{0}; chunk_id_left < _left_in_table->chunk_count(); ++chunk_id_left) {
      // initialize the data structures for left matches
      _left_matches[chunk_id_left].resize(_left_in_table->get_chunk(chunk_id_left)->size());
    }
  }

  auto left_data_type = _left_in_table->column_type(_left_column_id);
  auto right_data_type = _right_in_table->column_type(_right_column_id);

  _pos_list_left = std::make_shared<PosList>();
  _pos_list_right = std::make_shared<PosList>();

  size_t worst_case = _left_in_table->row_count() * _right_in_table->row_count();

  _pos_list_left->reserve(worst_case);
  _pos_list_right->reserve(worst_case);

  // Scan all chunks for right input
  for (ChunkID chunk_id_right = ChunkID{0}; chunk_id_right < _right_in_table->chunk_count(); ++chunk_id_right) {
    const auto chunk_right = _right_in_table->get_chunk(chunk_id_right);
    auto column_right = chunk_right->get_column(_right_column_id);
    const auto indices = chunk_right->get_indices(std::vector<ColumnID>{_right_column_id});
    _right_matches[chunk_id_right].resize(chunk_right->size());

    std::shared_ptr<BaseIndex> index = nullptr;

    if (indices.size() > 0) {
      // We assume the first index to be efficient for our join
      // as we do not want to spend time on evaluating the best index inside of this join loop
      index = indices.front();
    }

    // Scan all chunks from left input
    for (ChunkID chunk_id_left = ChunkID{0}; chunk_id_left < _left_in_table->chunk_count(); ++chunk_id_left) {
      auto chunk_column_left = _left_in_table->get_chunk(chunk_id_left)->get_column(_left_column_id);

      resolve_data_and_column_type(left_data_type, *chunk_column_left, [&](auto left_type, auto& typed_left_column) {
        using LeftType = typename decltype(left_type)::type;

        auto iterable_left = create_iterable_from_column<LeftType>(typed_left_column);

        if (index != nullptr) {
          // utilize index for join
          iterable_left.with_iterators([&](auto left_it, auto left_end) {
            this->_join_two_columns_using_index(left_it, left_end, chunk_id_left, chunk_id_right, index);
          });
        } else {
          // fallback to nested loop implementation
          resolve_data_and_column_type(right_data_type, *column_right, [&](auto right_type, auto& typed_right_column) {
            using RightType = typename decltype(right_type)::type;

            // make sure that we do not compile invalid versions of these lambdas
            constexpr auto left_is_string_column = (std::is_same<LeftType, std::string>{});
            constexpr auto right_is_string_column = (std::is_same<RightType, std::string>{});

            constexpr auto neither_is_string_column = !left_is_string_column && !right_is_string_column;
            constexpr auto both_are_string_columns = left_is_string_column && right_is_string_column;

            // clang-format off
            if constexpr (neither_is_string_column || both_are_string_columns) {
              auto iterable_right = create_iterable_from_column<RightType>(typed_right_column);

              iterable_left.with_iterators([&](auto left_it, auto left_end) {
                  iterable_right.with_iterators([&](auto right_it, auto right_end) {
                      with_comparator(_predicate_condition, [&](auto comparator) {
                          this->_join_two_columns_nested_loop(comparator, left_it, left_end, right_it, right_end,
                                                              chunk_id_left, chunk_id_right);
                      });
                  });
              });
            }
          });
        }
        // clang-format on
      });
    }
  }

  // For Full Outer and Left Join we need to add all unmatched rows for the left side
  if (_mode == JoinMode::Left || _mode == JoinMode::Outer) {
    for (ChunkID chunk_id_left = ChunkID{0}; chunk_id_left < _left_in_table->chunk_count(); ++chunk_id_left) {
      for (ChunkOffset chunk_offset{0}; chunk_offset < _left_matches[chunk_id_left].size(); ++chunk_offset) {
        if (!_left_matches[chunk_id_left][chunk_offset]) {
          _pos_list_left->emplace_back(RowID{chunk_id_left, chunk_offset});
          _pos_list_right->emplace_back(RowID{ChunkID{0}, INVALID_CHUNK_OFFSET});
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
          _pos_list_left->emplace_back(RowID{ChunkID{0}, INVALID_CHUNK_OFFSET});
        }
      }
    }
  }

  _pos_list_left->shrink_to_fit();
  _pos_list_right->shrink_to_fit();

  // write output chunks
  auto output_chunk = std::make_shared<Chunk>();

  _write_output_chunk(output_chunk, _left_in_table, _pos_list_left);
  _write_output_chunk(output_chunk, _right_in_table, _pos_list_right);

  _output_table->emplace_chunk(output_chunk);
}

// join loop that joins two chunks of two columns using an iterator for the left, and an index for the right
template <typename LeftIterator>
void JoinIndex::_join_two_columns_using_index(LeftIterator left_it, LeftIterator left_end, const ChunkID chunk_id_left,
                                              const ChunkID chunk_id_right, std::shared_ptr<BaseIndex> index) {
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

// join loop that joins two chunks of two columns via their iterators
template <typename BinaryFunctor, typename LeftIterator, typename RightIterator>
void JoinIndex::_join_two_columns_nested_loop(const BinaryFunctor& func, LeftIterator left_it, LeftIterator left_end,
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
  auto num_right_matches = std::distance(range_begin, range_end);

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

void JoinIndex::_write_output_chunk(std::shared_ptr<Chunk> output_chunk, const std::shared_ptr<const Table> input_table,
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
