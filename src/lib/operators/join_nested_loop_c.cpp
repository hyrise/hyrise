#include "join_nested_loop_c.hpp"

#include <map>
#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <set>
#include <vector>

#include "resolve_type.hpp"
#include "storage/base_attribute_vector.hpp"
#include "storage/column_visitable.hpp"
#include "storage/dictionary_column.hpp"
#include "storage/iterables/create_iterable_from_column.hpp"
#include "storage/reference_column.hpp"
#include "storage/value_column.hpp"
#include "type_comparison.hpp"
#include "utils/assert.hpp"

namespace opossum {

JoinNestedLoopC::JoinNestedLoopC(const std::shared_ptr<const AbstractOperator> left,
                                 const std::shared_ptr<const AbstractOperator> right, const JoinMode mode,
                                 const std::pair<ColumnID, ColumnID>& column_ids, const ScanType scan_type)
    : AbstractJoinOperator(left, right, mode, column_ids, scan_type) {}

const std::string JoinNestedLoopC::name() const { return "JoinNestedLoopC"; }

std::shared_ptr<AbstractOperator> JoinNestedLoopC::recreate(const std::vector<AllParameterVariant>& args) const {
  return std::make_shared<JoinNestedLoopC>(_input_left->recreate(args), _input_right->recreate(args), _mode,
                                           _column_ids, _scan_type);
}

std::shared_ptr<const Table> JoinNestedLoopC::_on_execute() {
  _output_table = std::make_shared<Table>();

  _left_in_table = _input_left->get_output();
  _right_in_table = _input_right->get_output();

  _left_column_id = _column_ids.first;
  _right_column_id = _column_ids.second;

  // Preparing output table by adding columns from left table
  for (ColumnID column_id{0}; column_id < _left_in_table->column_count(); ++column_id) {
    _output_table->add_column_definition(_left_in_table->column_name(column_id), _left_in_table->column_type(column_id),
                                         true);
  }

  // Preparing output table by adding columns from right table
  for (ColumnID column_id{0}; column_id < _right_in_table->column_count(); ++column_id) {
    _output_table->add_column_definition(_right_in_table->column_name(column_id),
                                         _right_in_table->column_type(column_id), true);
  }

  auto left_type_string = _left_in_table->column_type(_column_ids.first);
  auto right_type_string = _right_in_table->column_type(_column_ids.second);

  resolve_data_type(left_type_string, [&](auto left_type) {
    using LeftType = typename decltype(left_type)::type;

    resolve_data_type(right_type_string, [&](auto right_type) {
      using RightType = typename decltype(right_type)::type;

      if (_mode == JoinMode::Right) {
        this->_perform_join<LeftType, RightType>();
      } else {
        this->_perform_join<RightType, LeftType>();
      }

    });
  });

  return _output_table;
}

template <typename LeftType, typename RightType>
void JoinNestedLoopC::_perform_join() {
  std::function<bool(LeftType, RightType)> _comparator;

  // Parsing the join operator
  switch (_scan_type) {
    case ScanType::OpEquals: {
      _comparator = [](LeftType left_val, RightType right_val) { return value_equal(left_val, right_val); };
      break;
    }
    case ScanType::OpNotEquals: {
      _comparator = [](LeftType left_val, RightType right_val) { return !value_equal(left_val, right_val); };
      break;
    }
    case ScanType::OpLessThan: {
      _comparator = [](LeftType left_val, RightType right_val) { return value_smaller(left_val, right_val); };
      break;
    }
    case ScanType::OpLessThanEquals: {
      _comparator = [](LeftType left_val, RightType right_val) { return !value_greater(left_val, right_val); };
      break;
    }
    case ScanType::OpGreaterThan: {
      _comparator = [](LeftType left_val, RightType right_val) { return value_greater(left_val, right_val); };
      break;
    }
    case ScanType::OpGreaterThanEquals: {
      _comparator = [](LeftType left_val, RightType right_val) { return !value_smaller(left_val, right_val); };
      break;
    }
    default:
      Fail(std::string("Unsupported operator for join."));
  }

  auto left_table = _left_in_table;
  auto right_table = _right_in_table;

  auto left_column_id = _left_column_id;
  auto right_column_id = _right_column_id;

  if (_mode == JoinMode::Right) {
    // swap everything!
    left_table = _right_in_table;
    right_table = _left_in_table;

    left_column_id = _right_column_id;
    right_column_id = _left_column_id;
  }

  auto pos_list_left = std::make_shared<PosList>();
  auto pos_list_right = std::make_shared<PosList>();

  std::set<RowID> right_matches;

  // Scan all chunks from left input
  for (ChunkID chunk_id_left = ChunkID{0}; chunk_id_left < left_table->chunk_count(); ++chunk_id_left) {
    auto column_left = left_table->get_chunk(chunk_id_left).get_column(left_column_id);

    resolve_column_type<LeftType>(*column_left, [&](auto& typed_left_column) {
      auto iterable_left = create_iterable_from_column<LeftType>(typed_left_column);

      iterable_left.for_each([&](const auto& left_value) {
        bool left_found_match = false;

        // Scan all chunks for right input
        for (ChunkID chunk_id_right = ChunkID{0}; chunk_id_right < right_table->chunk_count(); ++chunk_id_right) {
          auto column_right = right_table->get_chunk(chunk_id_right).get_column(right_column_id);

          resolve_column_type<RightType>(*column_right, [&](auto& typed_right_column) {

            auto iterable_right = create_iterable_from_column<RightType>(typed_right_column);

            if (left_value.is_null()) return;

            iterable_right.for_each([&](const auto& right_value) {

              if (right_value.is_null()) return;

              if (_comparator(left_value.value(), right_value.value())) {
                pos_list_left->emplace_back(RowID{chunk_id_left, left_value.chunk_offset()});
                pos_list_right->emplace_back(RowID{chunk_id_right, right_value.chunk_offset()});

                left_found_match = true;

                if (_mode == JoinMode::Outer) {
                  right_matches.insert(RowID{chunk_id_right, right_value.chunk_offset()});
                }
              }
            });
          });
        }

        if (!left_found_match) {
          if (_mode == JoinMode::Left || _mode == JoinMode::Right || _mode == JoinMode::Outer) {
            pos_list_left->emplace_back(RowID{chunk_id_left, left_value.chunk_offset()});
            pos_list_right->emplace_back(RowID{ChunkID{0}, INVALID_CHUNK_OFFSET});
          }
        }
      });
    });
  }

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

  auto output_chunk = Chunk();

  if (_mode == JoinMode::Right) {
    _write_output_chunks(output_chunk, right_table, ChunkID{0}, pos_list_right);
    _write_output_chunks(output_chunk, left_table, ChunkID{0}, pos_list_left);
  } else {
    _write_output_chunks(output_chunk, left_table, ChunkID{0}, pos_list_left);
    _write_output_chunks(output_chunk, right_table, ChunkID{0}, pos_list_right);
  }

  _output_table->emplace_chunk(std::move(output_chunk));
}

void JoinNestedLoopC::_write_output_chunks(Chunk& output_chunk, const std::shared_ptr<const Table> input_table,
                                           ChunkID chunk_id, std::shared_ptr<PosList> pos_list) {
  // Add columns from left table to output chunk
  for (ColumnID column_id{0}; column_id < input_table->column_count(); ++column_id) {
    std::shared_ptr<BaseColumn> column;

    DebugAssert(chunk_id < input_table->chunk_count(), "Chunk id out of range");
    if (auto ref_col_left = std::dynamic_pointer_cast<const ReferenceColumn>(
            input_table->get_chunk(ChunkID{0}).get_column(column_id))) {
      auto new_pos_list = std::make_shared<PosList>();

      ChunkID current_chunk{0};

      // de-reference to the correct RowID so the output can be used in a Multi Join
      for (const auto& row : *pos_list) {
        if (row.chunk_id != current_chunk) {
          current_chunk = row.chunk_id;

          ref_col_left = std::dynamic_pointer_cast<const ReferenceColumn>(
              input_table->get_chunk(current_chunk).get_column(column_id));
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
