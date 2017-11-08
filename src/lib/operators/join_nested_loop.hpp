#pragma once

#include <functional>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "abstract_join_operator.hpp"
#include "type_comparison.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

class JoinNestedLoop : public AbstractJoinOperator {
 public:
  JoinNestedLoop(const std::shared_ptr<const AbstractOperator> left,
                 const std::shared_ptr<const AbstractOperator> right, const JoinMode mode,
                 const std::pair<ColumnID, ColumnID>& column_ids, const ScanType scan_type);

  const std::string name() const override;
  std::shared_ptr<AbstractOperator> recreate(const std::vector<AllParameterVariant>& args = {}) const override;

 protected:
  std::shared_ptr<const Table> _on_execute() override;

  void _perform_join();

  void _create_table_structure();

  void _write_output_chunks(Chunk& output_chunk, const std::shared_ptr<const Table> input_table,
                            std::shared_ptr<PosList> pos_list);

  std::shared_ptr<Table> _output_table;
  std::shared_ptr<const Table> _left_in_table;
  std::shared_ptr<const Table> _right_in_table;
  ColumnID _left_column_id;
  ColumnID _right_column_id;

  bool _is_outer_join;
  std::shared_ptr<PosList> _pos_list_left;
  std::shared_ptr<PosList> _pos_list_right;

  // for Full Outer, remember the matches on the right side
  std::set<RowID> _right_matches;

  // Static function that calls a given functor with the correct std comparator
  template <typename Functor>
  static void _with_operator(const ScanType scan_type, const Functor& func) {
    switch (scan_type) {
      case ScanType::OpEquals:
        func(std::equal_to<void>{});
        break;

      case ScanType::OpNotEquals:
        func(std::not_equal_to<void>{});
        break;

      case ScanType::OpLessThan:
        func(std::less<void>{});
        break;

      case ScanType::OpLessThanEquals:
        func(std::less_equal<void>{});
        break;

      case ScanType::OpGreaterThan:
        func(std::greater<void>{});
        break;

      case ScanType::OpGreaterThanEquals:
        func(std::greater_equal<void>{});
        break;

      default:
        Fail("Unsupported operator.");
    }
  }

  // inner join loop that joins two columns via their iterators
  template <typename BinaryFunctor, typename LeftIterator, typename RightIterator>
  void _join_two_columns(const BinaryFunctor& func, LeftIterator left_it, LeftIterator left_end,
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
};

}  // namespace opossum
