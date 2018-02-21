#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_rule.hpp"

namespace opossum {

class AbstractLQPNode;
class ProjectionNode;

/**
 * This optimizer rule combines consecutive ProjectionNodes if possible.
 */
class ProjectionCombinationRule : public AbstractRule {
 public:
  std::string name() const override;
  bool apply_to(const std::shared_ptr<AbstractLQPNode>& node) override;

 private:
  std::shared_ptr<ProjectionNode> _combine_projections(std::vector<std::shared_ptr<ProjectionNode>>& projections) const;
};

}  // namespace opossum
