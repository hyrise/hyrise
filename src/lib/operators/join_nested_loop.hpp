#pragma once

#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "abstract_join_operator.hpp"
#include "types.hpp"

namespace opossum {

class JoinNestedLoop : public AbstractJoinOperator {
 public:
  JoinNestedLoop(const std::shared_ptr<const AbstractOperator> left,
                 const std::shared_ptr<const AbstractOperator> right, const JoinMode mode,
                 const ColumnIDPair& column_ids, const PredicateCondition predicate_condition);

  const std::string name() const override;
  std::shared_ptr<AbstractOperator> _on_recreate(
      const std::vector<AllParameterVariant>& args, const std::shared_ptr<AbstractOperator>& recreated_input_left,
      const std::shared_ptr<AbstractOperator>& recreated_input_right) const override;

 protected:
  std::shared_ptr<const Table> _on_execute() override;

  void _perform_join();

  template <typename BinaryFunctor, typename LeftIterator, typename RightIterator>
  void _join_two_columns(const BinaryFunctor& func, LeftIterator left_it, LeftIterator left_end,
                         RightIterator right_begin, RightIterator right_end, const ChunkID chunk_id_left,
                         const ChunkID chunk_id_right, std::vector<bool>& left_matches);

  void _create_table_structure();

  void _write_output_chunks(ChunkColumns& columns, const std::shared_ptr<const Table> input_table,
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
};

}  // namespace opossum
