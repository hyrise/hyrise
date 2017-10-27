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
  JoinNestedLoopB(const std::shared_ptr<const AbstractOperator> left,
                  const std::shared_ptr<const AbstractOperator> right, const JoinMode mode,
                  const std::pair<ColumnID, ColumnID>& column_ids, const ScanType scan_type);

  const std::string name() const override;
  uint8_t num_in_tables() const override;
  uint8_t num_out_tables() const override;
  std::shared_ptr<AbstractOperator> recreate(const std::vector<AllParameterVariant>& args = {}) const override;

 protected:
  struct JoinContext;

  template <typename T>
  class JoinNestedLoopBImpl;

  std::shared_ptr<const Table> _on_execute() override;

  void _add_outer_join_rows(std::shared_ptr<const Table> outer_side_table, std::shared_ptr<PosList> outer_side_pos_list,
                            std::set<RowID>& outer_side_matches, std::shared_ptr<PosList> null_side_pos_list);
  void _join_columns(ColumnID left_column_id, ColumnID right_column_id, std::string left_column_type);
  std::shared_ptr<PosList> _dereference_pos_list(std::shared_ptr<const Table> input_table, ColumnID column_id,
                                                 std::shared_ptr<const PosList> pos_list);
  void _append_columns_to_output(std::shared_ptr<const Table> input_table, std::shared_ptr<PosList> pos_list);

  // Output fields
  std::shared_ptr<PosList> _pos_list_left;
  std::set<RowID> _left_match;
  std::shared_ptr<PosList> _pos_list_right;
  std::set<RowID> _right_match;
  std::shared_ptr<Table> _output;
};
}  // namespace opossum
