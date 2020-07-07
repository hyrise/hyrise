#pragma once

#include <optional>
#include <string>
#include <vector>

#include "abstract_non_query_node.hpp"

namespace opossum {

/**
 * This node type represents a dummy table that is used to project literals.
 * See Projection::DummyTable for more details.
 */
class DummyTableNode : public EnableMakeForLQPNode<DummyTableNode>, public AbstractNonQueryNode {
 public:
  DummyTableNode();

  std::string description(const DescriptionMode mode = DescriptionMode::Short) const override;

 protected:
  std::shared_ptr<AbstractLQPNode> _on_shallow_copy(LQPNodeMapping& node_mapping) const override;
  bool _on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const override;
};

}  // namespace opossum
