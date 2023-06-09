#pragma once

#include "abstract_rule.hpp"

namespace hyrise {

class SemiJoinReductionRemovalRule : public AbstractRule {
 public:
  std::string name() const override;

 protected:
  void _apply_to_plan_without_subqueries(const std::shared_ptr<AbstractLQPNode>& lqp_root) const override;
};

}  // namespace hyrise
