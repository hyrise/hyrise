#pragma once

#include <string>

#include "abstract_lqp_node.hpp"

namespace opossum {

/**
 * This node type represents the SHOW TABLES management command.
 */
class ShowTablesNode : public EnableMakeForLQPNode<ShowTablesNode>, public AbstractLQPNode {
 public:
  ShowTablesNode();

  std::string description() const override;

  const std::vector<std::shared_ptr<AbstractExpression>>& column_expressions() const override;
  bool is_column_nullable(const ColumnID column_id) const override;

 protected:
  std::shared_ptr<AbstractLQPNode> _on_shallow_copy(LQPNodeMapping& node_mapping) const override;
  bool _on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const override;

 private:
  mutable std::optional<std::vector<std::shared_ptr<AbstractExpression>>> _column_expressions;
};

}  // namespace opossum
