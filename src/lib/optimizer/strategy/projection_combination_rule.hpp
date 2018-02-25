#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_rule.hpp"

namespace opossum {

class AbstractLQPNode;
class ProjectionNode;

/**
 * This optimizer rule combines consecutive ProjectionNodes if they all have exactly one child/parent
 * (Note: The node furthest up in the tree, the "root" of a ProjectionNode sequence,
 * can have multiple parents; the one furthest down in the sequence can have multiple children).
 *
 * The rule removes all but the root ProjectionNode of the sequence from the tree.
 * If the root ProjectionNode contains any LQPColumnReferences to a column in one of the ProjectionNodes
 * to be removed, then the LQPColumnReference is replaced by the actual column.
 */
class ProjectionCombinationRule : public AbstractRule {
 public:
  std::string name() const override;
  bool apply_to(const std::shared_ptr<AbstractLQPNode>& node) override;

 private:
  std::shared_ptr<ProjectionNode> _combine_projections(std::vector<std::shared_ptr<ProjectionNode>>& projections) const;
};

}  // namespace opossum
