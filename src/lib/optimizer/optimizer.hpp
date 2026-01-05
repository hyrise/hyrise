#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "cost_estimation/cost_estimator_logical.hpp"
#include "optimizer/strategy/abstract_rule.hpp"
#include "statistics/cardinality_estimator.hpp"

namespace hyrise {

struct OptimizerRuleMetrics {
  explicit OptimizerRuleMetrics(const std::string& init_rule_name, const std::chrono::nanoseconds init_duration);
  std::string rule_name;
  std::chrono::nanoseconds duration;
};

class AbstractRule;
class AbstractLQPNode;

/**
 * Applies optimization rules to an LQP.
 * On each invocation of optimize(), these Batches are applied in the same order as they were added
 * to the Optimizer.
 *
 * Optimizer::create_default_optimizer() creates the Optimizer with the default rule set.
 */
class Optimizer final {
 public:
  static std::shared_ptr<Optimizer> create_default_optimizer(bool with_optimizations_for_data_dependencies = false);

  static std::shared_ptr<Optimizer> create_default_optimizer_with_cardinality_estimator();

  explicit Optimizer(const std::shared_ptr<AbstractCostEstimator>& cost_estimator =
                         std::make_shared<CostEstimatorLogical>(std::make_shared<CardinalityEstimator>()));

  /**
   * Add @param rule to the Optimizers rule set. The rule will be set to use the Optimizer's _cost_estimator
   */
  void add_rule(std::unique_ptr<AbstractRule> rule);

  /**
   * Returns optimized version of @param input LQP. Wraps `optimize_with_context()` and returns only the optimized
   * LQP. @param rule_durations may be set in order to retrieve runtime information for each applied rule.
   */
  std::shared_ptr<AbstractLQPNode> optimize(
      std::shared_ptr<AbstractLQPNode> input,
      const std::shared_ptr<std::vector<OptimizerRuleMetrics>>& rule_durations = nullptr) const;

  /**
   * Returns optimized version of @param input LQP and the OptimizationContext used by all transformation rules.
   * @param rule_durations may be set in order to retrieve runtime information for each applied rule.
   * The OptimizationContext contains metadata about the optimization process, such as whether the resulting LQP
   * is cacheable or not.
   */
  std::pair<std::shared_ptr<AbstractLQPNode>, std::unique_ptr<OptimizationContext>> optimize_with_context(
      std::shared_ptr<AbstractLQPNode> input,
      const std::shared_ptr<std::vector<OptimizerRuleMetrics>>& rule_durations = nullptr) const;

  static void validate_lqp(const std::shared_ptr<AbstractLQPNode>& root_node);

  Cardinality estimate_cardinality(std::shared_ptr<const AbstractLQPNode>& lqp);

  CardinalityEstimator& cardinality_estimator();

 private:
  std::vector<std::unique_ptr<AbstractRule>> _rules;
  std::shared_ptr<AbstractCostEstimator> _cost_estimator;
};

}  // namespace hyrise
