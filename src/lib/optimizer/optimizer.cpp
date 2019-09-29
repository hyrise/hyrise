#include "optimizer.hpp"

#include <memory>
#include <unordered_set>

#include "cost_estimation/cost_estimator_logical.hpp"
#include "expression/expression_utils.hpp"
#include "expression/lqp_subquery_expression.hpp"
#include "logical_query_plan/logical_plan_root_node.hpp"
#include "strategy/between_composition_rule.hpp"
#include "strategy/chunk_pruning_rule.hpp"
#include "strategy/column_pruning_rule.hpp"
#include "strategy/expression_reduction_rule.hpp"
#include "strategy/in_expression_rewrite_rule.hpp"
#include "strategy/index_scan_rule.hpp"
#include "strategy/insert_limit_in_exists_rule.hpp"
#include "strategy/join_ordering_rule.hpp"
#include "strategy/join_predicate_ordering_rule.hpp"
#include "strategy/predicate_merge_rule.hpp"
#include "strategy/predicate_placement_rule.hpp"
#include "strategy/predicate_reordering_rule.hpp"
#include "strategy/predicate_split_up_rule.hpp"
#include "strategy/subquery_to_join_rule.hpp"

/**
 * IMPORTANT NOTES ON OPTIMIZING SUBQUERY LQPS
 *
 * Multiple Expressions in different nodes might reference the same LQP. Most commonly this will be the case for a
 * ProjectionNode computing a subquery and a subsequent PredicateNode filtering based on it.
 * We do not WANT to optimize the LQP twice (optimization takes time after all) and we CANNOT optimize it twice, since,
 * e.g., a non-deterministic rule, could produce two different LQPs while optimizing and then the SubqueryExpression
 * in the PredicateNode couldn't be resolved to a column anymore. There are more subtle ways LQPs might break in this
 * scenario, and frankly, this is one of the weak links in the expression system...
 *
 * ...long story short:
 * !!!
 * EACH UNIQUE SUB-LQP IS ONLY OPTIMIZED ONCE, EVEN IF IT OCCURS IN DIFFERENT NODES/EXPRESSIONS.
 * !!!
 *
 * -> collect_subquery_expressions_by_lqp() identifies unique LQPs and the (multiple) SubqueryExpressions referencing
 *                                          each of these unique LQPs.
 *
 * -> Optimizer::_apply_rule()              optimizes each unique LQP exactly once and assigns the optimized LQPs back
 *                                          to the SubqueryExpressions referencing them.
 */

namespace {

using namespace opossum;  // NOLINT

// All SubqueryExpressions referencing the same LQP
using SubqueryExpressionsByLQP =
    std::vector<std::pair<std::shared_ptr<AbstractLQPNode>, std::vector<std::shared_ptr<LQPSubqueryExpression>>>>;

// See comment at the top of file for the purpose of this.
void collect_subquery_expressions_by_lqp(SubqueryExpressionsByLQP& subquery_expressions_by_lqp,
                                         const std::shared_ptr<AbstractLQPNode>& node,
                                         std::unordered_set<std::shared_ptr<AbstractLQPNode>>& visited_nodes) {
  if (!node) return;
  if (!visited_nodes.emplace(node).second) return;

  for (const auto& expression : node->node_expressions) {
    visit_expression(expression, [&](const auto& sub_expression) {
      const auto subquery_expression = std::dynamic_pointer_cast<LQPSubqueryExpression>(sub_expression);
      if (!subquery_expression) return ExpressionVisitation::VisitArguments;

      for (auto& [lqp, subquery_expressions] : subquery_expressions_by_lqp) {
        if (*lqp == *subquery_expression->lqp) {
          subquery_expressions.emplace_back(subquery_expression);
          return ExpressionVisitation::DoNotVisitArguments;
        }
      }
      subquery_expressions_by_lqp.emplace_back(subquery_expression->lqp, std::vector{subquery_expression});

      return ExpressionVisitation::DoNotVisitArguments;
    });
  }

  collect_subquery_expressions_by_lqp(subquery_expressions_by_lqp, node->left_input(), visited_nodes);
  collect_subquery_expressions_by_lqp(subquery_expressions_by_lqp, node->right_input(), visited_nodes);
}

}  // namespace

namespace opossum {

std::shared_ptr<Optimizer> Optimizer::create_default_optimizer() {
  const auto optimizer = std::make_shared<Optimizer>();

  optimizer->add_rule(std::make_unique<ExpressionReductionRule>());

  optimizer->add_rule(std::make_unique<ColumnPruningRule>());

  optimizer->add_rule(std::make_unique<ChunkPruningRule>());

  // Run before the JoinOrderingRule so that the latter has simple (non-conjunctive) predicates. However, as the
  // JoinOrderingRule cannot handle UnionNodes (#1829), do not split disjunctions just yet.
  optimizer->add_rule(std::make_unique<PredicateSplitUpRule>(false));

  // The JoinOrderingRule cannot proceed past Semi/Anti Joins. These may be part of the initial query plan (in which
  // case we are out of luck and the join ordering will be sub-optimal) but many of them are also introduced by the
  // SubqueryToJoinRule. As such, we run the JoinOrderingRule before the SubqueryToJoinRule.
  optimizer->add_rule(std::make_unique<JoinOrderingRule>());

  optimizer->add_rule(std::make_unique<BetweenCompositionRule>());

  // Position the predicates after the JoinOrderingRule ran. The JOR manipulates predicate placement as well, but
  // for now we want the PredicateReorderingRule to have the final say on predicate positions
  optimizer->add_rule(std::make_unique<PredicatePlacementRule>());

  optimizer->add_rule(std::make_unique<PredicateSplitUpRule>());

  optimizer->add_rule(std::make_unique<SubqueryToJoinRule>());

  optimizer->add_rule(std::make_unique<JoinPredicateOrderingRule>());

  optimizer->add_rule(std::make_unique<InsertLimitInExistsRule>());

  // Prune chunks after the BetweenCompositionRule ran, as `a >= 5 AND a <= 7` may not be prunable predicates while
  // `a BETWEEN 5 and 7` is. Also, run it after the PredicatePlacementRule, so that predicates are as close to the
  // StoredTableNode as possible where the ChunkPruningRule can work with them.
  optimizer->add_rule(std::make_unique<ChunkPruningRule>());

  // Bring predicates into the desired order once the PredicatePlacementRule has positioned them as desired
  optimizer->add_rule(std::make_unique<PredicateReorderingRule>());

  // Before the IN predicate is rewritten, it should have been moved to a good position. Also, while the IN predicate
  // might become a join, it is semantically more similar to a predicate. If we run this rule too early, it might
  // hinder other optimizations that stop at joins. For example, the join ordering currently does not know about semi
  // joins and would not recognize such a rewritten predicate.
  optimizer->add_rule(std::make_unique<InExpressionRewriteRule>());

  optimizer->add_rule(std::make_unique<IndexScanRule>());

  optimizer->add_rule(std::make_unique<PredicateMergeRule>());

  return optimizer;
}

Optimizer::Optimizer(const std::shared_ptr<AbstractCostEstimator>& cost_estimator) : _cost_estimator(cost_estimator) {}

void Optimizer::add_rule(std::unique_ptr<AbstractRule> rule) {
  rule->cost_estimator = _cost_estimator;
  _rules.emplace_back(std::move(rule));
}

std::shared_ptr<AbstractLQPNode> Optimizer::optimize(const std::shared_ptr<AbstractLQPNode>& input) const {
  // Add explicit root node, so the rules can freely change the tree below it without having to maintain a root node
  // to return to the Optimizer
  const auto root_node = LogicalPlanRootNode::make(input);

  for (const auto& rule : _rules) {
    _apply_rule(*rule, root_node);
  }

  // Remove LogicalPlanRootNode
  const auto optimized_node = root_node->left_input();
  root_node->set_left_input(nullptr);

  return optimized_node;
}

void Optimizer::_apply_rule(const AbstractRule& rule, const std::shared_ptr<AbstractLQPNode>& root_node) const {
  rule.apply_to(root_node);

  /**
   * Optimize Subqueries
   */
  auto subquery_expressions_by_lqp = SubqueryExpressionsByLQP{};
  auto visited_nodes = std::unordered_set<std::shared_ptr<AbstractLQPNode>>{};
  collect_subquery_expressions_by_lqp(subquery_expressions_by_lqp, root_node, visited_nodes);

  for (const auto& [lqp, subquery_expressions] : subquery_expressions_by_lqp) {
    const auto local_root_node = LogicalPlanRootNode::make(lqp);
    _apply_rule(rule, local_root_node);
    for (const auto& subquery_expression : subquery_expressions) {
      subquery_expression->lqp = local_root_node->left_input();
    }
  }
}

}  // namespace opossum
