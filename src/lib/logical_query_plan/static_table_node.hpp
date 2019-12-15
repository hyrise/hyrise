#pragma once

#include "base_non_query_node.hpp"
#include "enable_make_for_lqp_node.hpp"
#include "storage/table.hpp"
#include "storage/table_column_definition.hpp"

namespace opossum {

/**
 * This node type wraps a table and can be used as input for a CreateTableNode to represent a simple
 * CREATE TABLE management command.
 */
class StaticTableNode : public EnableMakeForLQPNode<StaticTableNode>, public BaseNonQueryNode {
 public:
  explicit StaticTableNode(const std::shared_ptr<Table>& table);

  std::string description(const DescriptionMode mode = DescriptionMode::Short) const override;

  const std::vector<std::shared_ptr<AbstractExpression>>& column_expressions() const override;
  bool is_column_nullable(const ColumnID column_id) const override;

  const std::shared_ptr<Table> table;

 protected:
  mutable std::optional<std::vector<std::shared_ptr<AbstractExpression>>> _column_expressions;

  size_t _on_shallow_hash() const override;
  std::shared_ptr<AbstractLQPNode> _on_shallow_copy(LQPNodeMapping& node_mapping) const override;
  bool _on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const override;
};

}  // namespace opossum
