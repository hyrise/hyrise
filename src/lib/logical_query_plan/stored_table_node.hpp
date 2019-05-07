#pragma once

#include <optional>
#include <vector>

#include "abstract_lqp_node.hpp"
#include "expression/abstract_expression.hpp"
#include "lqp_column_reference.hpp"

namespace opossum {

class LQPColumnExpression;

/**
 * Represents a Table from the StorageManager in an LQP
 *
 * Holds Column and Chunk pruning information.
 */
class StoredTableNode : public EnableMakeForLQPNode<StoredTableNode>, public AbstractLQPNode {
 public:
  explicit StoredTableNode(const std::string& table_name);

  LQPColumnReference get_column(const std::string& name) const;

  /**
   * @defgroup ColumnIDs and ChunkIDs to be pruned from the stored Table.
   * Both vectors need to be sorted and unique when passed to `set_excluded_{chunk/column}_ids()`
   *
   * @{
   */
  void set_excluded_chunk_ids(const std::vector<ChunkID>& excluded_chunk_ids);
  const std::vector<ChunkID>& excluded_chunk_ids() const;

  void set_excluded_column_ids(const std::vector<ColumnID>& excluded_column_ids);
  const std::vector<ColumnID>& excluded_column_ids() const;
  /** @} */

  std::string description() const override;
  const std::vector<std::shared_ptr<AbstractExpression>>& column_expressions() const override;
  bool is_column_nullable(const ColumnID column_id) const override;
  std::shared_ptr<TableStatistics> derive_statistics_from(
      const std::shared_ptr<AbstractLQPNode>& left_input,
      const std::shared_ptr<AbstractLQPNode>& right_input) const override;

  const std::string table_name;

 protected:
  std::shared_ptr<AbstractLQPNode> _on_shallow_copy(LQPNodeMapping& node_mapping) const override;
  bool _on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const override;

 private:
  mutable std::optional<std::vector<std::shared_ptr<AbstractExpression>>> _column_expressions;
  std::vector<ChunkID> _excluded_chunk_ids;
  std::vector<ColumnID> _excluded_column_ids;
};

}  // namespace opossum
