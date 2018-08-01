#pragma once

#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "abstract_join_operator.hpp"
#include "types.hpp"

namespace opossum {

class JoinIndex;

class JoinNestedLoop : public AbstractJoinOperator {
 public:
  JoinNestedLoop(const std::shared_ptr<const AbstractOperator>& left,
                 const std::shared_ptr<const AbstractOperator>& right, const JoinMode mode,
                 const ColumnIDPair& column_ids, const PredicateCondition predicate_condition);

  const std::string name() const override;

 protected:
  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_input_left,
      const std::shared_ptr<AbstractOperator>& copied_input_right) const override;
  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

  struct JoinParams {
    PosList& pos_list_left;
    PosList& pos_list_right;
    std::vector<bool>& left_matches;
    std::vector<bool>& right_matches;
    const bool track_left_matches;
    const bool track_right_matches;
    const JoinMode mode;
    const PredicateCondition predicate_condition;
  };

 protected:
  std::shared_ptr<const Table> _on_execute() override;

  void _perform_join();

  // Having all these static methods and passing around the state of the JoinNestedLoop is somewhat ugly, but it allows
  // us to reuse the code in JoinIndex as a fallback. __attribute__((noinline)) is simply magic - otherwise the
  // compiler would try to put the entire join for all types into a single, monolithic function. For -O3 on clang, this
  // reduces the compile time from twelve minutes to less than three.

  static void __attribute__((noinline))
  _join_two_untyped_columns(const std::shared_ptr<const BaseColumn>& column_left,
                            const std::shared_ptr<const BaseColumn>& column_right, const ChunkID chunk_id_left,
                            const ChunkID chunk_id_right, JoinParams& params);

  template <typename BinaryFunctor, typename LeftIterator, typename RightIterator>
  static void __attribute__((noinline))
  _join_two_typed_columns(const BinaryFunctor& func, LeftIterator left_it, LeftIterator left_end,
                          RightIterator right_begin, RightIterator right_end, const ChunkID chunk_id_left,
                          const ChunkID chunk_id_right, JoinParams& params);

  static void _process_match(RowID left_row_id, RowID right_row_id, JoinParams& params);

  void _create_table_structure();

  void _write_output_chunks(ChunkColumns& columns, const std::shared_ptr<const Table>& input_table,
                            const std::shared_ptr<PosList>& pos_list);

  void _on_cleanup() override;

  std::shared_ptr<Table> _output_table;
  std::shared_ptr<const Table> _left_in_table;
  std::shared_ptr<const Table> _right_in_table;
  ColumnID _left_column_id;
  ColumnID _right_column_id;

  bool _is_outer_join{false};
  std::shared_ptr<PosList> _pos_list_left;
  std::shared_ptr<PosList> _pos_list_right;

  // for Full Outer, remember the matches on the right side
  std::vector<std::vector<bool>> _right_matches;

  // The JoinIndex uses this join as a fallback if no index exists
  friend class JoinIndex;
};

}  // namespace opossum
