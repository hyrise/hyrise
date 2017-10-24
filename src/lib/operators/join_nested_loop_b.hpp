#pragma once

#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "abstract_join_operator.hpp"
#include "storage/column_visitable.hpp"
#include "storage/dictionary_column.hpp"
#include "storage/value_column.hpp"
#include "types.hpp"

namespace opossum {

/**
 * There are two nested loop joins, implemented by two groups: JoinNestedLoopA and B. They should be functionally
 * identical.
 *
 * Note: JoinNestedLoopB does not support null values in input tables at the moment
 */
class JoinNestedLoopB : public AbstractJoinOperator {
 public:
  JoinNestedLoopB(const std::shared_ptr<const AbstractOperator>& left,
                  const std::shared_ptr<const AbstractOperator>& right, const JoinMode mode,
                  const std::pair<ColumnID, ColumnID>& column_ids, const ScanType scan_type);

  const std::string name() const override;
  uint8_t num_in_tables() const override;
  uint8_t num_out_tables() const override;
  std::shared_ptr<AbstractOperator> recreate(const std::vector<AllParameterVariant>& args) const override;

 protected:
  std::shared_ptr<const Table> _on_execute() override;

  struct JoinContext : ColumnVisitableContext {
    JoinContext(const std::shared_ptr<const BaseColumn>& column_left,
                const std::shared_ptr<const BaseColumn>& column_right, ChunkID left_chunk_id, ChunkID right_chunk_id,
                JoinMode mode)
        : _column_left{column_left},
          _column_right{column_right},
          _left_chunk_id{left_chunk_id},
          _right_chunk_id{right_chunk_id},
          _mode{mode} {};

    std::shared_ptr<const BaseColumn> _column_left;
    std::shared_ptr<const BaseColumn> _column_right;
    ChunkID _left_chunk_id;
    ChunkID _right_chunk_id;
    JoinMode _mode;
  };

  template <typename T>
  class JoinNestedLoopBImpl : public AbstractJoinOperatorImpl, public ColumnVisitable {
   public:
    JoinNestedLoopBImpl<T>(JoinNestedLoopB& join_nested_loop_b);

    // AbstractOperatorImpl implementation
    std::shared_ptr<const Table> _on_execute() override;

    // ColumnVisitable implementation
    void handle_value_column(const BaseValueColumn& column, std::shared_ptr<ColumnVisitableContext> context) override;
    void handle_dictionary_column(const BaseDictionaryColumn& column,
                                  std::shared_ptr<ColumnVisitableContext> context) override;
    void handle_reference_column(const ReferenceColumn& column,
                                 std::shared_ptr<ColumnVisitableContext> context) override;

    void join_value_value(const ValueColumn<T>& left, const ValueColumn<T>& right,
                          const std::shared_ptr<JoinContext>& context, bool reverse_order = false);
    void join_value_dictionary(const ValueColumn<T>& left, const DictionaryColumn<T>& right,
                               const std::shared_ptr<JoinContext>& context, bool reverse_order = false);
    void join_value_reference(const ValueColumn<T>& left, const ReferenceColumn& right,
                              const std::shared_ptr<JoinContext>& context, bool reverse_order = false);
    void join_dictionary_dictionary(const DictionaryColumn<T>& left, const DictionaryColumn<T>& right,
                                    const std::shared_ptr<JoinContext>& context, bool reverse_order = false);
    void join_dictionary_reference(const DictionaryColumn<T>& left, const ReferenceColumn& right,
                                   const std::shared_ptr<JoinContext>& context, bool reverse_order = false);
    void join_reference_reference(const ReferenceColumn& left, const ReferenceColumn& right,
                                  const std::shared_ptr<JoinContext>& context, bool reverse_order = false);

   protected:
    JoinNestedLoopB& _join_nested_loop_b;
    std::function<bool(const T&, const T&)> _compare;
    void _match_values(const T& value_left, ChunkOffset left_chunk_offset, const T& value_right,
                       ChunkOffset right_chunk_offset, const std::shared_ptr<JoinContext>& context, bool reverse_order);
    const T& _resolve_reference(const ReferenceColumn& ref_column, ChunkOffset chunk_offset);
  };

  void _add_outer_join_rows(const std::shared_ptr<const Table>& outer_side_table,
                            const std::shared_ptr<PosList>& outer_side_pos_list, std::set<RowID>& outer_side_matches,
                            const std::shared_ptr<PosList>& null_side_pos_list);
  void _join_columns(ColumnID left_column_id, ColumnID right_column_id, std::string left_column_type);
  std::shared_ptr<PosList> _dereference_pos_list(const std::shared_ptr<const Table>& input_table, ColumnID column_id,
                                                 const std::shared_ptr<const PosList>& pos_list);
  void _append_columns_to_output(const std::shared_ptr<const Table>& input_table,
                                 const std::shared_ptr<PosList>& pos_list);

  // Output fields
  std::shared_ptr<PosList> _pos_list_left;
  std::set<RowID> _left_match;
  std::shared_ptr<PosList> _pos_list_right;
  std::set<RowID> _right_match;
  std::shared_ptr<Table> _output;
};
}  // namespace opossum
