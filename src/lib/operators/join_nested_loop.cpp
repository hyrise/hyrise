#include "join_nested_loop.hpp"

#include <map>
#include <memory>
#include <numeric>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "resolve_type.hpp"
#include "storage/iterables/create_iterable_from_column.hpp"
#include "type_comparison.hpp"
#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"

namespace opossum {

/*
 * This is a Nested Loop Join implementation completely based on iterables.
 * It supports all current join and scan types, as well as NULL values.
 * Because this is a Nested Loop Join, the performance is going to be far inferior to JoinHash and JoinSortMerge,
 * so only use this for testing or benchmarking purposes.
 */

JoinNestedLoop::JoinNestedLoop(const std::shared_ptr<const AbstractOperator> left,
                               const std::shared_ptr<const AbstractOperator> right, const JoinMode mode,
                               const std::pair<ColumnID, ColumnID>& column_ids, const ScanType scan_type)
    : AbstractJoinOperator(left, right, mode, column_ids, scan_type) {}

const std::string JoinNestedLoop::name() const { return "JoinNestedLoop"; }

std::shared_ptr<AbstractOperator> JoinNestedLoop::recreate(const std::vector<AllParameterVariant>& args) const {
  return std::make_shared<JoinNestedLoop>(_input_left->recreate(args), _input_right->recreate(args), _mode, _column_ids,
                                          _scan_type);
}

std::shared_ptr<const Table> JoinNestedLoop::_on_execute() {
  PerformanceWarning("Nested Loop Join used");

  _prepare_output();

  _left_column_id = _column_ids.first;
  _right_column_id = _column_ids.second;

  // Resolve both column data types and execute the main join loop
  resolve_data_type(_left_in_table->column_type(_left_column_id), [&](auto left_type) {
    using LeftType = typename decltype(left_type)::type;

    resolve_data_type(_right_in_table->column_type(_right_column_id), [&](auto right_type) {
      using RightType = typename decltype(right_type)::type;

      // for the right outer join we switch types because we treat the "left" side as the outer relation
      if (_mode == JoinMode::Right) {
        this->_perform_join<RightType, LeftType>();
      } else {
        this->_perform_join<LeftType, RightType>();
      }
    });
  });

  return _output_table;
}

void JoinNestedLoop::_prepare_output() {
  _output_table = std::make_shared<Table>();

  _left_in_table = _input_left->get_output();
  _right_in_table = _input_right->get_output();

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

template <typename LeftType, typename RightType>
void JoinNestedLoop::_perform_join() {
  std::function<bool(LeftType, RightType)> _comparator;

  // Parsing the join operator
  switch (_scan_type) {
    case ScanType::OpEquals:
      _comparator = [](LeftType left_val, RightType right_val) { return value_equal(left_val, right_val); };
      break;
    case ScanType::OpNotEquals:
      _comparator = [](LeftType left_val, RightType right_val) { return !value_equal(left_val, right_val); };
      break;
    case ScanType::OpLessThan:
      _comparator = [](LeftType left_val, RightType right_val) { return value_smaller(left_val, right_val); };
      break;
    case ScanType::OpLessThanEquals:
      _comparator = [](LeftType left_val, RightType right_val) { return !value_greater(left_val, right_val); };
      break;
    case ScanType::OpGreaterThan:
      _comparator = [](LeftType left_val, RightType right_val) { return value_greater(left_val, right_val); };
      break;
    case ScanType::OpGreaterThanEquals:
      _comparator = [](LeftType left_val, RightType right_val) { return !value_smaller(left_val, right_val); };
      break;

    default:
      Fail(std::string("Unsupported operator for join."));
  }

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

  auto pos_list_left = std::make_shared<PosList>();
  auto pos_list_right = std::make_shared<PosList>();

  // for Full Outer, remember the matches on the right side
  std::set<RowID> right_matches;

  // Scan all chunks from left input
  for (ChunkID chunk_id_left = ChunkID{0}; chunk_id_left < left_table->chunk_count(); ++chunk_id_left) {
    auto column_left = left_table->get_chunk(chunk_id_left).get_column(left_column_id);

    resolve_column_type<LeftType>(*column_left, [&](auto& typed_left_column) {
      auto iterable_left = create_iterable_from_column<LeftType>(typed_left_column);

      // for Outer joins, remember matches on the left side
      std::vector<bool> left_matches;

      if (_mode == JoinMode::Left || _mode == JoinMode::Right || _mode == JoinMode::Outer) {
        left_matches.resize(typed_left_column.size());
      }

      // Scan all chunks for right input
      for (ChunkID chunk_id_right = ChunkID{0}; chunk_id_right < right_table->chunk_count(); ++chunk_id_right) {
        auto column_right = right_table->get_chunk(chunk_id_right).get_column(right_column_id);

        resolve_column_type<RightType>(*column_right, [&](auto& typed_right_column) {
          auto iterable_right = create_iterable_from_column<RightType>(typed_right_column);

          iterable_left.for_each([&](const auto& left_value) {
            if (left_value.is_null()) return;

            iterable_right.for_each([&](const auto& right_value) {
              if (right_value.is_null()) return;

              if (_comparator(left_value.value(), right_value.value())) {
                pos_list_left->emplace_back(RowID{chunk_id_left, left_value.chunk_offset()});
                pos_list_right->emplace_back(RowID{chunk_id_right, right_value.chunk_offset()});

                left_matches[left_value.chunk_offset()] = true;

                if (_mode == JoinMode::Outer) {
                  right_matches.insert(RowID{chunk_id_right, right_value.chunk_offset()});
                }
              }
            });
          });
        });
      }

      if (_mode == JoinMode::Left || _mode == JoinMode::Right || _mode == JoinMode::Outer) {
        // add unmatched rows for outer joins
        for (ChunkOffset chunk_offset{0}; chunk_offset < left_matches.size(); ++chunk_offset) {
          if (!left_matches[chunk_offset]) {
            pos_list_left->emplace_back(RowID{chunk_id_left, chunk_offset});
            pos_list_right->emplace_back(RowID{ChunkID{0}, INVALID_CHUNK_OFFSET});
          }
        }
      }
    });
  }

  // For Full Outer we need to add all unmatched rows for the right side.
  // Unmatched rows on the left side are already added in the main loop above
  if (_mode == JoinMode::Outer) {
    for (ChunkID chunk_id_right = ChunkID{0}; chunk_id_right < right_table->chunk_count(); ++chunk_id_right) {
      auto column_right = right_table->get_chunk(chunk_id_right).get_column(right_column_id);

      resolve_column_type<RightType>(*column_right, [&](auto& typed_right_column) {
        auto iterable_right = create_iterable_from_column<RightType>(typed_right_column);

        iterable_right.for_each([&](const auto& right_value) {
          const auto row_id = RowID{chunk_id_right, right_value.chunk_offset()};
          if (!right_matches.count(row_id)) {
            pos_list_left->emplace_back(RowID{ChunkID{0}, INVALID_CHUNK_OFFSET});
            pos_list_right->emplace_back(row_id);
          }
        });
      });
    }
  }

  // write output chunks
  auto output_chunk = Chunk();

  if (_mode == JoinMode::Right) {
    _write_output_chunks(output_chunk, right_table, pos_list_right);
    _write_output_chunks(output_chunk, left_table, pos_list_left);
  } else {
    _write_output_chunks(output_chunk, left_table, pos_list_left);
    _write_output_chunks(output_chunk, right_table, pos_list_right);
  }

  _output_table->emplace_chunk(std::move(output_chunk));
}

void JoinNestedLoop::_write_output_chunks(Chunk& output_chunk, const std::shared_ptr<const Table> input_table,
                                          std::shared_ptr<PosList> pos_list) {
  // Add columns from table to output chunk
  for (ColumnID column_id{0}; column_id < input_table->column_count(); ++column_id) {
    std::shared_ptr<BaseColumn> column;

    DebugAssert(chunk_id < input_table->chunk_count(), "Chunk id out of range");
    if (auto ref_col_left = std::dynamic_pointer_cast<const ReferenceColumn>(
            input_table->get_chunk(ChunkID{0}).get_column(column_id))) {
      auto new_pos_list = std::make_shared<PosList>();

      ChunkID current_chunk_id{0};

      // de-reference to the correct RowID so the output can be used in a Multi Join
      for (const auto& row : *pos_list) {
        if (row.chunk_id != current_chunk_id) {
          current_chunk_id = row.chunk_id;

          ref_col_left = std::dynamic_pointer_cast<const ReferenceColumn>(
              input_table->get_chunk(current_chunk_id).get_column(column_id));
        }
        new_pos_list->push_back(ref_col_left->pos_list()->at(row.chunk_offset));
      }

      column = std::make_shared<ReferenceColumn>(ref_col_left->referenced_table(), ref_col_left->referenced_column_id(),
                                                 new_pos_list);
    } else {
      column = std::make_shared<ReferenceColumn>(input_table, column_id, pos_list);
    }

    output_chunk.add_column(column);
  }
}

}  // namespace opossum
