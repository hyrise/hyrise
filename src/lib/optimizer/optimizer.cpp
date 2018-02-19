#include "optimizer.hpp"

#include <memory>

#include "logical_query_plan/logical_plan_root_node.hpp"
#include "strategy/constant_calculation_rule.hpp"
#include "strategy/index_scan_rule.hpp"
#include "strategy/join_detection_rule.hpp"
#include "strategy/predicate_reordering_rule.hpp"

namespace opossum {

std::shared_ptr<Optimizer> Optimizer::create_default_optimizer() {
  auto optimizer = std::make_shared<Optimizer>(10);

  RuleBatch main_batch(RuleBatchExecutionPolicy::Iterative);

  main_batch.add_rule(std::make_shared<PredicateReorderingRule>());
  main_batch.add_rule(std::make_shared<JoinDetectionRule>());
  optimizer->add_rule_batch(main_batch);

  RuleBatch final_batch(RuleBatchExecutionPolicy::Once);
  final_batch.add_rule(std::make_shared<ConstantCalculationRule>());
  final_batch.add_rule(std::make_shared<IndexScanRule>());
  optimizer->add_rule_batch(final_batch);

  return optimizer;
}

Optimizer::Optimizer(const uint32_t max_num_iterations) : _max_num_iterations(max_num_iterations) {}

void Optimizer::add_rule_batch(RuleBatch rule_batch) { _rule_batches.emplace_back(std::move(rule_batch)); }

std::shared_ptr<AbstractLQPNode> Optimizer::optimize(const std::shared_ptr<AbstractLQPNode>& input) const {
  // Add explicit root node, so the rules can freely change the tree below it without having to maintain a root node
  // to return to the Optimizer
  const auto root_node = std::make_shared<LogicalPlanRootNode>();
  root_node->set_left_child(input);

  for (const auto& rule_batch : _rule_batches) {
    switch (rule_batch.execution_policy()) {
      case RuleBatchExecutionPolicy::Once:
        rule_batch.apply_rules_to(root_node);
        break;

      case RuleBatchExecutionPolicy::Iterative:
        /**
         * Apply all optimization over and over until all of them stopped changing the LQP or the max number of
         * iterations is reached
         */
        for (uint32_t iter_index = 0; iter_index < _max_num_iterations; ++iter_index) {
          if (!rule_batch.apply_rules_to(root_node)) break;
        }
        break;
    }
  }

  // Remove LogicalPlanRootNode
  const auto optimized_node = root_node->left_child();
  optimized_node->clear_parents();

  return optimized_node;
}

}  // namespace opossum
