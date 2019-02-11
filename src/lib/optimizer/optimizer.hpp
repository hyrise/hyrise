#pragma once

#include <memory>
#include <vector>

#include "cost_model/cost_model_logical.hpp"
#include "optimizer/strategy/abstract_rule.hpp"
#include "statistics/cardinality_estimator.hpp"

namespace opossum {

class AbstractRule;
class AbstractLQPNode;
class OptimizationContext;

/**
 * Applies optimization rules to an LQP.
 * On each invocation of optimize(), these Batches are applied in the same order as they were added
 * to the Optimizer.
 *
 * Optimizer::create_default_optimizer() creates the Optimizer with the default rule set.
 */
class Optimizer final {
 public:
  static std::shared_ptr<Optimizer> create_default_optimizer();

  explicit Optimizer(const std::shared_ptr<AbstractCostEstimator>& cost_estimator = std::make_shared<CostModelLogical>(std::make_shared<CardinalityEstimator>()));

  void add_rule(const std::shared_ptr<AbstractRule>& rule);

  std::shared_ptr<AbstractLQPNode> optimize(const std::shared_ptr<AbstractLQPNode>& input) const;

 private:
  std::vector<std::shared_ptr<AbstractRule>> _rules;
  std::shared_ptr<AbstractCostEstimator> _cost_estimator;

  void _apply_rule(const AbstractRule& rule, const std::shared_ptr<AbstractLQPNode>& root_node) const;
};

}  // namespace opossum
