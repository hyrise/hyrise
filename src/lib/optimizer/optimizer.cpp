#include "optimizer.hpp"

#include <memory>
#include <unordered_set>

#include "cost_model/cost_model_logical.hpp"
#include "expression/expression_utils.hpp"
#include "expression/lqp_select_expression.hpp"
#include "logical_query_plan/logical_plan_root_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "optimizer/strategy/predicate_placement_rule.hpp"
#include "optimizer/strategy/subselect_to_join_reformulation_rule.hpp"
#include "strategy/chunk_pruning_rule.hpp"
#include "strategy/column_pruning_rule.hpp"
#include "strategy/constant_calculation_rule.hpp"
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

  for (const auto& expression : node->node_expressions) {
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
  auto optimizer = std::make_shared<Optimizer>();

  // Run pruning just once since the rule would otherwise insert the pruning ProjectionNodes multiple times.
  optimizer->add_rule(std::make_shared<ConstantCalculationRule>());

  optimizer->add_rule(std::make_shared<LogicalReductionRule>());

  // Relies on being run after LogicalReductionRule (to avoid handling ANDed expressions) and before ColumnPruningRule
  // (to avoid implementing to much logic when removing projections).
  optimizer->add_rule(std::make_shared<SubselectToJoinReformulationRule>());

  optimizer->add_rule(std::make_shared<ColumnPruningRule>());

  optimizer->add_rule(std::make_shared<ChunkPruningRule>());

  optimizer->add_rule(std::make_shared<JoinOrderingRule>(std::make_shared<CostModelLogical>()));

  // Position the predicates after the JoinOrderingRule ran. The JOR manipulates predicate placement as well, but
  // for now we want the PredicateReorderingRule to have the final say on predicate positions
  optimizer->add_rule(std::make_shared<PredicatePlacementRule>());

  // Bring predicates into the desired order once the PredicateReorderingRule has positioned them as desired
  optimizer->add_rule(std::make_shared<PredicateReorderingRule>());

  optimizer->add_rule(std::make_shared<IndexScanRule>());

  return optimizer;
}

void Optimizer::add_rule(const std::shared_ptr<AbstractRule>& rule) { _rules.emplace_back(rule); }

std::shared_ptr<AbstractLQPNode> Optimizer::optimize(const std::shared_ptr<AbstractLQPNode>& input) const {
  // Add explicit root node, so the rules can freely change the tree below it without having to maintain a root node
  // to return to the Optimizer
  const auto root_node = LogicalPlanRootNode::make(input);

  for (const auto& rule : _rules) {
    _apply_rule(*rule, root_node);
  }

  // Remove LogicalPlanRootNode
  const auto optimized_node = root_node->left_input();
  optimized_node->clear_outputs();

  return optimized_node;
}

void Optimizer::_apply_rule(const AbstractRule& rule, const std::shared_ptr<AbstractLQPNode>& root_node) const {
  rule.apply_to(root_node);

  /**
   * Optimize Subselects
   */
  auto select_expressions_by_lqp = SelectExpressionsByLQP{};
  auto visited_nodes = std::unordered_set<std::shared_ptr<AbstractLQPNode>>{};
  collect_select_expressions_by_lqp(select_expressions_by_lqp, root_node, visited_nodes);

  for (const auto& lqp_and_select_expressions : select_expressions_by_lqp) {
    const auto local_root_node = LogicalPlanRootNode::make(lqp_and_select_expressions.first);
    _apply_rule(rule, local_root_node);
    for (const auto& select_expression : lqp_and_select_expressions.second) {
      select_expression->lqp = local_root_node->left_input();
    }
  }
}

}  // namespace opossum
