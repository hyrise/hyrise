#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_lqp_node.hpp"

namespace opossum {

class AbstractExpression;

/**
 * Node type to represent updates (i.e., invalidation and inserts) in a table.
 */
class UpdateNode : public EnableMakeForLQPNode<UpdateNode>, public AbstractLQPNode {
 public:
  UpdateNode(const std::string& table_name,
             const std::vector<std::shared_ptr<AbstractExpression>>& update_column_expressions);

  std::string description() const override;
  size_t node_expression_count() const override;
  std::shared_ptr<AbstractExpression>& node_expression(const size_t idx) override;

  const std::string table_name;
  std::vector<std::shared_ptr<AbstractExpression>> update_column_expressions;

 protected:
  std::shared_ptr<AbstractLQPNode> _on_shallow_copy(LQPNodeMapping& node_mapping) const override;
  bool _on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const override;
};

}  // namespace opossum
