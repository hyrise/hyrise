#include "optimizer.hpp"

#include <memory>
#include <unordered_set>

#include "cost_model/cost_model_logical.hpp"
#include "expression/expression_utils.hpp"
#include "expression/lqp_select_expression.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/logical_plan_root_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "optimizer/optimization_context.hpp"
#include "optimizer/strategy/predicate_placement_rule.hpp"
#include "statistics/cardinality_estimator.hpp"
#include "strategy/chunk_pruning_rule.hpp"
#include "strategy/column_pruning_rule.hpp"
#include "strategy/constant_calculation_rule.hpp"
#include "strategy/exists_reformulation_rule.hpp"
#include "strategy/index_scan_rule.hpp"
#include "strategy/join_detection_rule.hpp"
#include "strategy/join_ordering_rule.hpp"
#include "strategy/logical_reduction_rule.hpp"
#include "strategy/predicate_reordering_rule.hpp"
#include "utils/performance_warning.hpp"

/**
 * IMPORTANT NOTES ON OPTIMIZING SUB-SELECT LQPS
 *
 * Multiple Expressions in different nodes might reference the same LQP. Most commonly this will be the case for a
 * ProjectionNode computing a subselect and a subsequent PredicateNode filtering based on it.
 * We do not WANT to optimize the LQP twice (optimization takes time after all) and we CANNOT optimize it twice, since,
 * e.g., a non-deterministic rule, could produce two different LQPs while optimizing and then the select-expression
 * in the PredicateNode couldn't be resolved to a column anymore. There are more subtle ways LQPs might break in this
 * scenario, and frankly, this is one of the weak links in the expression system...
 *
 * ...long story short:
 * !!!
 * EACH UNIQUE SUB-LQP IS ONLY OPTIMIZED ONCE, EVEN IF IT OCCURS IN DIFFERENT NODES/EXPRESSIONS.
 * !!!
 *
 * -> collect_select_expressions_by_lqp()   identifies unique LQPs and the (multiple) SelectExpressions referencing
 *                                          each of these unique LQPs.
 *
 * -> Optimizer::_apply_rule()              optimizes each unique LQP exactly once and assignes the optimized LQPs back
 *                                          to the SelectExpressions referencing them.
 */

namespace {

using namespace opossum;  // NOLINT

// All SelectExpressions referencing the same LQP
using SelectExpressionsByLQP =
    std::vector<std::pair<std::shared_ptr<AbstractLQPNode>, std::vector<std::shared_ptr<LQPSelectExpression>>>>;

// See comment at the top of file for the purpose of this.
void collect_select_expressions_by_lqp(SelectExpressionsByLQP& select_expressions_by_lqp,
                                       const std::shared_ptr<AbstractLQPNode>& node,
                                       std::unordered_set<std::shared_ptr<AbstractLQPNode>>& visited_nodes) {
  if (!node) return;
  if (!visited_nodes.emplace(node).second) return;

  for (const auto& expression : node->node_expressions()) {
    visit_expression(expression, [&](const auto& sub_expression) {
      const auto lqp_select_expression = std::dynamic_pointer_cast<LQPSelectExpression>(sub_expression);
      if (!lqp_select_expression) return ExpressionVisitation::VisitArguments;

      for (auto& lqp_and_select_expressions : select_expressions_by_lqp) {
        if (*lqp_and_select_expressions.first == *lqp_select_expression->lqp) {
          lqp_and_select_expressions.second.emplace_back(lqp_select_expression);
          return ExpressionVisitation::DoNotVisitArguments;
        }
      }
      select_expressions_by_lqp.emplace_back(lqp_select_expression->lqp, std::vector{lqp_select_expression});

      return ExpressionVisitation::DoNotVisitArguments;
    });
  }

  collect_select_expressions_by_lqp(select_expressions_by_lqp, node->left_input(), visited_nodes);
  collect_select_expressions_by_lqp(select_expressions_by_lqp, node->right_input(), visited_nodes);
}

void populate_context(const std::shared_ptr<AbstractLQPNode>& plan,
                      const std::shared_ptr<OptimizationContext>& context) {
  visit_lqp(plan, [&](const auto& node) {
    if (node->input_count() == 0) {
      context->plan_leaf_indices.emplace(node, context->plan_leaf_indices.size());
    }

    if (const auto join_node = std::dynamic_pointer_cast<JoinNode>(node)) {
      if (join_node->join_predicate) {
        context->predicate_indices.emplace(join_node->join_predicate, context->predicate_indices.size());
      }
    } else if (const auto predicate_node = std::dynamic_pointer_cast<PredicateNode>(node)) {
      context->predicate_indices.emplace(predicate_node->predicate, context->predicate_indices.size());
    }

    for (const auto& node_expression : node->node_expressions()) {
      visit_expression(node_expression, [&](const auto& sub_expression) {
        if (const auto select_expression = std::dynamic_pointer_cast<LQPSelectExpression>(sub_expression)) {
          populate_context(select_expression->lqp, context);
        }
        return ExpressionVisitation::VisitArguments;
      });
    }

    return LQPVisitation::VisitInputs;
  });
}

}  // namespace

namespace opossum {

std::shared_ptr<Optimizer> Optimizer::create_default_optimizer() {
  auto optimizer = std::make_shared<Optimizer>(100);

  RuleBatch final_batch(RuleBatchExecutionPolicy::Once);

  // Run pruning just once since the rule would otherwise insert the pruning ProjectionNodes multiple times.
  final_batch.add_rule(std::make_shared<ConstantCalculationRule>());

  final_batch.add_rule(std::make_shared<LogicalReductionRule>());

  //final_batch.add_rule(std::make_shared<ColumnPruningRule>());

  final_batch.add_rule(std::make_shared<ExistsReformulationRule>());

  final_batch.add_rule(std::make_shared<ChunkPruningRule>());

  final_batch.add_rule(std::make_shared<JoinOrderingRule>());

  // Position the predicates after the JoinOrderingRule ran. The JOR manipulates predicate placement as well, but
  // for now we want the PredicateReorderingRule to have the final say on predicate positions
  final_batch.add_rule(std::make_shared<PredicatePlacementRule>());

  // Bring predicates into the desired order once the PredicateReorderingRule has positioned them as desired
  //final_batch.add_rule(std::make_shared<PredicateReorderingRule>());

  final_batch.add_rule(std::make_shared<IndexScanRule>());

  optimizer->add_rule_batch(final_batch);

  return optimizer;
}

std::shared_ptr<OptimizationContext> Optimizer::create_optimization_context(
    const std::shared_ptr<AbstractLQPNode>& plan) {
  const auto context = std::make_shared<OptimizationContext>();

  populate_context(plan, context);

  return context;
}

Optimizer::Optimizer(const uint32_t max_num_iterations, const std::shared_ptr<AbstractCostEstimator>& cost_estimator)
    : _max_num_iterations(max_num_iterations), _cost_estimator(cost_estimator) {}

void Optimizer::add_rule_batch(RuleBatch rule_batch) { _rule_batches.emplace_back(std::move(rule_batch)); }

std::shared_ptr<AbstractLQPNode> Optimizer::optimize(const std::shared_ptr<AbstractLQPNode>& input) const {
  // Add explicit root node, so the rules can freely change the tree below it without having to maintain a root node
  // to return to the Optimizer
  const auto root_node = LogicalPlanRootNode::make(input);

  const auto context = create_optimization_context(input);
  context->print();

  for (const auto& rule_batch : _rule_batches) {
    switch (rule_batch.execution_policy()) {
      case RuleBatchExecutionPolicy::Once:
        _apply_rule_batch(rule_batch, root_node, context);
        break;

      case RuleBatchExecutionPolicy::Iterative:
        /**
         * Apply all optimization over and over until all of them stopped changing the LQP or the max number of
         * iterations is reached
         */
        auto iter_index = uint32_t{0};
        for (; iter_index < _max_num_iterations; ++iter_index) {
          if (!_apply_rule_batch(rule_batch, root_node, context)) {
            break;
          }
        }
        if (iter_index == _max_num_iterations) {
          PerformanceWarning("Maximum number of optimizer iterations reached");
        }
        break;
    }
  }

  // Remove LogicalPlanRootNode
  const auto optimized_node = root_node->left_input();
  optimized_node->clear_outputs();

  return optimized_node;
}

bool Optimizer::_apply_rule_batch(const RuleBatch& rule_batch, const std::shared_ptr<AbstractLQPNode>& root_node,
                                  const std::shared_ptr<OptimizationContext>& context) const {
  auto lqp_changed = false;

  for (auto& rule : rule_batch.rules()) {
    lqp_changed |= _apply_rule(*rule, root_node, context);
  }

  return lqp_changed;
}

bool Optimizer::_apply_rule(const AbstractRule& rule, const std::shared_ptr<AbstractLQPNode>& root_node,
                            const std::shared_ptr<OptimizationContext>& context) const {
  auto lqp_changed = rule.apply_to(root_node, *_cost_estimator, context);

  /**
   * Optimize Subselects
   */
  auto select_expressions_by_lqp = SelectExpressionsByLQP{};
  auto visited_nodes = std::unordered_set<std::shared_ptr<AbstractLQPNode>>{};
  collect_select_expressions_by_lqp(select_expressions_by_lqp, root_node, visited_nodes);

  for (const auto& lqp_and_select_expressions : select_expressions_by_lqp) {
    const auto root_node = LogicalPlanRootNode::make(lqp_and_select_expressions.first);
    lqp_changed |= _apply_rule(rule, root_node, context);
    for (const auto& select_expression : lqp_and_select_expressions.second) {
      select_expression->lqp = root_node->left_input();
    }

    // Explicitly untie the root node, otherwise the LQP is left with an expired output weak_ptr
    root_node->set_left_input(nullptr);
  }

  return lqp_changed;
}

}  // namespace opossum
