#pragma once

#include <optional>
#include <string>
#include <vector>

#include "abstract_lqp_node.hpp"

namespace opossum {

/**
 * This node type represents a dummy table that is used to project literals.
 * See Projection::DummyTable for more details.
 */
class DummyTableNode : public EnableMakeForLQPNode<DummyTableNode>, public AbstractLQPNode {
 public:
  DummyTableNode();

  std::string description() const override;

  bool shallow_equals(const AbstractLQPNode& rhs) const override;

  const std::vector<std::string>& output_column_names() const override;

 protected:
  std::shared_ptr<AbstractLQPNode> _deep_copy_impl(
      const std::shared_ptr<AbstractLQPNode>& copied_left_input,
      const std::shared_ptr<AbstractLQPNode>& copied_right_input) const override;
  std::vector<std::string> _output_column_names;
};

}  // namespace opossum
