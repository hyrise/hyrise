#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_rule.hpp"

namespace hyrise {

class AbstractLQPNode;
class PredicateNode;

// This rule removes PredicateNodes that hold IsNull expressions if the scanned columns are known to not be nullable.
// It does not yet deal with IsNotNull predicates or cases where Is(Not)Null is nested within another expression.
class NullScanRemovalRule : public AbstractRule {
 public:
  void apply_to_plan(const std::shared_ptr<LogicalPlanRootNode>& root) const override;
  std::string name() const override;

 private:
  static void _remove_nodes(const std::vector<std::shared_ptr<AbstractLQPNode>>& nodes);

 protected:
  void _apply_to_plan_without_subqueries(const std::shared_ptr<AbstractLQPNode>& lqp_root) const override;
};

}  // namespace hyrise
