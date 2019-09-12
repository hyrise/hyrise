#pragma once

#include "cost_estimation/abstract_cost_estimator.hpp"
#include "optimizer/strategy/abstract_rule.hpp"
#include "types.hpp"

namespace opossum {
class AbstractLQPNode;
class JoinNode;

/**
 * This optimizer rule tries to find the best join implementation of a join node.
 */
class JoinAlgorithmRule : public AbstractRule {
 public:
  explicit JoinAlgorithmRule(const std::shared_ptr<AbstractCostEstimator>& cost_estimator);

  void apply_to(const std::shared_ptr<AbstractLQPNode>& node) const override;

 private:
  //        std::shared_ptr<PredicateNode> _find_predicate_for_cross_join(const std::shared_ptr<JoinNode>& cross_join) const;
  const std::vector<JoinType> _valid_join_types(const std::shared_ptr<JoinNode>& node) const;
  const std::shared_ptr<AbstractCostEstimator> _cost_estimator;
};

}  // namespace opossum
