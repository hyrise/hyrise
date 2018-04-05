#pragma once

#include <string>

#include "abstract_lqp_node.hpp"

namespace opossum {

/**
 * This node type represents the CREATE VIEW management command.
 */
class CreateViewNode : public AbstractLQPNode {
 public:
  explicit CreateViewNode(const std::string& view_name, const AbstractLQPNodeCSPtr& lqp);

  std::string description() const override;
  const std::vector<std::string>& output_column_names() const override;

  bool shallow_equals(const AbstractLQPNode& rhs) const override;

  std::string view_name() const;
  AbstractLQPNodeCSPtr lqp() const;

 protected:
  AbstractLQPNodeSPtr _deep_copy_impl(
      const AbstractLQPNodeSPtr& copied_left_input,
      const AbstractLQPNodeSPtr& copied_right_input) const override;
  const std::string _view_name;
  const AbstractLQPNodeCSPtr _lqp;
};

}  // namespace opossum
