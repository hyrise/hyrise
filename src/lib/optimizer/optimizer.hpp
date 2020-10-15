#pragma once

#include <algorithm>
#include <memory>
#include <vector>

#include "cost_estimation/cost_estimator_logical.hpp"
#include "optimizer/strategy/abstract_rule.hpp"
#include "statistics/cardinality_estimator.hpp"

namespace opossum {

struct OptimizerRuleMetrics {
  std::string rule_name;
  std::chrono::nanoseconds duration;
};

class AbstractRule;
class AbstractLQPNode;

/**
 * Applies optimization rules to an LQP.
 * On each invocation of optimize(), these Batches are applied in the same order as they were added
 * to the Optimizer.
 */
class Optimizer final {
 public:
  static std::shared_ptr<Optimizer> create_default_optimizer();
  /**
   * This Optimizer applies specific optimizer rules that cannot be executed before caching
   */
  static std::shared_ptr<Optimizer> create_post_caching_optimizer();

  explicit Optimizer(const std::shared_ptr<AbstractCostEstimator>& cost_estimator =
                         std::make_shared<CostEstimatorLogical>(std::make_shared<CardinalityEstimator>()));

  /**
   * Add @param rule to the Optimizers rule set. The rule will be set to use the Optimizer's _cost_estimator
   */
  void add_rule(std::unique_ptr<AbstractRule> rule);

  /**
   * Returns optimized version of @param input.
   * @param rule_durations may be set in order to retrieve runtime information for each applied rule.
   */
  std::shared_ptr<AbstractLQPNode> optimize(
      std::shared_ptr<AbstractLQPNode> input,
      const std::shared_ptr<std::vector<OptimizerRuleMetrics>>& rule_durations = nullptr,
      const std::shared_ptr<bool>& cacheable = std::make_shared<bool>(true)) const;

  static void validate_lqp(const std::shared_ptr<AbstractLQPNode>& root_node);

 private:
  std::vector<std::unique_ptr<AbstractRule>> _rules;
  std::shared_ptr<AbstractCostEstimator> _cost_estimator;

  void _apply_rule(const AbstractRule& rule, const std::shared_ptr<AbstractLQPNode>& root_node) const;
};

}  // namespace opossum
