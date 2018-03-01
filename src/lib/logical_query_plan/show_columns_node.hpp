#pragma once

#include <string>

#include "abstract_lqp_node.hpp"

namespace opossum {

/**
 * This node type represents the SHOW COLUMNS management command.
 */
class ShowColumnsNode : public EnableMakeForLQPNode<ShowColumnsNode>, public AbstractLQPNode {
 public:
  explicit ShowColumnsNode(const std::string& table_name);

  std::string description() const override;

  const std::string& table_name() const;

  const std::vector<std::string>& output_column_names() const override;
  bool shallow_equals(const AbstractLQPNode& rhs) const override;

 protected:
  std::shared_ptr<AbstractLQPNode> _deep_copy_impl(
      const std::shared_ptr<AbstractLQPNode>& copied_left_input,
      const std::shared_ptr<AbstractLQPNode>& copied_right_input) const override;

 private:
  const std::string _table_name;
};

}  // namespace opossum
