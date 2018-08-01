#include "optimizer.hpp"

#include <memory>
#include <unordered_set>

#include "expression/expression_utils.hpp"
#include "expression/lqp_select_expression.hpp"
#include "logical_query_plan/logical_plan_root_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "strategy/chunk_pruning_rule.hpp"
#include "strategy/constant_calculation_rule.hpp"
#include "strategy/index_scan_rule.hpp"
#include "strategy/join_detection_rule.hpp"
#include "strategy/predicate_pushdown_rule.hpp"
#include "strategy/predicate_reordering_rule.hpp"

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

}  // namespace

namespace opossum {

std::shared_ptr<Optimizer> Optimizer::create_default_optimizer() {
  auto optimizer = std::make_shared<Optimizer>(10);

  RuleBatch main_batch(RuleBatchExecutionPolicy::Iterative);
  main_batch.add_rule(std::make_shared<PredicatePushdownRule>());
  main_batch.add_rule(std::make_shared<PredicateReorderingRule>());
  main_batch.add_rule(std::make_shared<JoinDetectionRule>());
  optimizer->add_rule_batch(main_batch);

  RuleBatch final_batch(RuleBatchExecutionPolicy::Once);
  final_batch.add_rule(std::make_shared<ChunkPruningRule>());
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
  const auto root_node = LogicalPlanRootNode::make(input);

  for (const auto& rule_batch : _rule_batches) {
    switch (rule_batch.execution_policy()) {
      case RuleBatchExecutionPolicy::Once:
        _apply_rule_batch(rule_batch, root_node);
        break;

      case RuleBatchExecutionPolicy::Iterative:
        /**
         * Apply all optimization over and over until all of them stopped changing the LQP or the max number of
         * iterations is reached
         */
        for (uint32_t iter_index = 0; iter_index < _max_num_iterations; ++iter_index) {
          if (!_apply_rule_batch(rule_batch, root_node)) break;
        }
        break;
    }
  }

  // Remove LogicalPlanRootNode
  const auto optimized_node = root_node->left_input();
  optimized_node->clear_outputs();

  return optimized_node;
}

bool Optimizer::_apply_rule_batch(const RuleBatch& rule_batch,
                                  const std::shared_ptr<AbstractLQPNode>& root_node) const {
  auto lqp_changed = false;

  for (auto& rule : rule_batch.rules()) {
    lqp_changed |= _apply_rule(*rule, root_node);
  }

  return lqp_changed;
}

bool Optimizer::_apply_rule(const AbstractRule& rule, const std::shared_ptr<AbstractLQPNode>& root_node) const {
  auto lqp_changed = rule.apply_to(root_node);

  /**
   * Optimize Subselects
   */
  auto select_expressions_by_lqp = SelectExpressionsByLQP{};
  auto visited_nodes = std::unordered_set<std::shared_ptr<AbstractLQPNode>>{};
  collect_select_expressions_by_lqp(select_expressions_by_lqp, root_node, visited_nodes);

  for (const auto& lqp_and_select_expressions : select_expressions_by_lqp) {
    const auto root_node = LogicalPlanRootNode::make(lqp_and_select_expressions.first);
    lqp_changed |= _apply_rule(rule, root_node);
    for (const auto& select_expression : lqp_and_select_expressions.second) {
      select_expression->lqp = root_node->left_input();
    }

    // Explicitly untie the root node, otherwise the LQP is left with an expired output weak_ptr
    root_node->set_left_input(nullptr);
  }

  return lqp_changed;
}

}  // namespace opossum
