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

  bool shallow_equals(const AbstractLQPNode& rhs) const override;

  const std::vector<std::string>& output_column_names() const override;

 protected:
  std::shared_ptr<AbstractLQPNode> _deep_copy_impl(
      const std::shared_ptr<AbstractLQPNode>& copied_left_input,
      const std::shared_ptr<AbstractLQPNode>& copied_right_input) const override;
};

}  // namespace opossum
