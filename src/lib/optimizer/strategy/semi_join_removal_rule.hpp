#pragma once

#include "abstract_rule.hpp"

namespace opossum {

class AbstractLQPNode;
class PredicateNode;

class SemiJoinRemovalRule : public AbstractRule {
 public:
  void _apply_to_plan_without_subqueries(const std::shared_ptr<AbstractLQPNode>& lqp_root) const override;

  constexpr static auto MINIMUM_SELECTIVITY = .25;
};

}  // namespace opossum
