#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_rule.hpp"

namespace opossum {

class AbstractLQPNode;
class ProjectionNode;

/**
 * This optimizer rule combines consecutive ProjectionNodes if they all have exactly one parent
 * (except for the node closest to the tree root in a sequence of ProjectionNodes, which can have multiple parents).
 *
 * The rule removes all but the ProjectionNode closest to the tree root (the top node of that sequence) from the tree.
 * If the top ProjectionNode contains any LQPColumnReferences to a column created by one of the ProjectionNodes
 * to be removed, then the LQPColumnReference is replaced by the actual column.
 */
class ProjectionCombinationRule : public AbstractRule {
 public:
  std::string name() const override;
  bool apply_to(const std::shared_ptr<AbstractLQPNode>& node) override;

 private:
  std::shared_ptr<ProjectionNode> _combine_projection_nodes(
      std::vector<std::shared_ptr<ProjectionNode>>& projection_nodes) const;
};

}  // namespace opossum
