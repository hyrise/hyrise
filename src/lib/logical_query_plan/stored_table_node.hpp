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
   * Both vectors need to be sorted and must no contain duplicates when passed to `set_pruned_{chunk/column}_ids()`
   *
   * @{
   */
  void set_pruned_chunk_ids(const std::vector<ChunkID>& pruned_chunk_ids);
  const std::vector<ChunkID>& pruned_chunk_ids() const;

  void set_pruned_column_ids(const std::vector<ColumnID>& pruned_column_ids);
  const std::vector<ColumnID>& pruned_column_ids() const;
  /** @} */

  std::string description() const override;
  const std::vector<std::shared_ptr<AbstractExpression>>& column_expressions() const override;
  bool is_column_nullable(const ColumnID column_id) const override;

  const std::string table_name;

 protected:
  std::shared_ptr<AbstractLQPNode> _on_shallow_copy(LQPNodeMapping& node_mapping) const override;
  bool _on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const override;

 private:
  mutable std::optional<std::vector<std::shared_ptr<AbstractExpression>>> _column_expressions;
  std::vector<ChunkID> _pruned_chunk_ids;
  std::vector<ColumnID> _pruned_column_ids;
};

}  // namespace opossum
