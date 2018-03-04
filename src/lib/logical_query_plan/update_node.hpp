#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_lqp_node.hpp"

namespace opossum {

class LQPExpression;

/**
 * Node type to represent updates (i.e., invalidation and inserts) in a table.
 */
class UpdateNode : public EnableMakeForLQPNode<UpdateNode>, public AbstractLQPNode {
 public:
  explicit UpdateNode(const std::string& table_name,
                      const std::vector<std::shared_ptr<LQPExpression>>& column_expressions);

  std::string description() const override;
  bool subplan_is_read_only() const override;

  const std::string& table_name() const;

  const std::vector<std::shared_ptr<LQPExpression>>& column_expressions() const;

  bool shallow_equals(const AbstractLQPNode& rhs) const override;

 protected:
  std::shared_ptr<AbstractLQPNode> _deep_copy_impl(
      const std::shared_ptr<AbstractLQPNode>& copied_left_input,
      const std::shared_ptr<AbstractLQPNode>& copied_right_input) const override;
  const std::string _table_name;
  std::vector<std::shared_ptr<LQPExpression>> _column_expressions;
};

}  // namespace opossum
