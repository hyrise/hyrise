#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_rule.hpp"

namespace opossum {

class AbstractLQPNode;
class PredicateNode;

class NullScanRemovalRule : public AbstractRule {
 public:
  void apply_to_plan(const std::shared_ptr<LogicalPlanRootNode>& root) const override;

 private:
  void _remove_nodes(const std::vector<std::shared_ptr<AbstractLQPNode>>& nodes) const;

 protected:
  void _apply_to_plan_without_subqueries(const std::shared_ptr<AbstractLQPNode>& lqp_root) const override;
};

}  // namespace opossum
