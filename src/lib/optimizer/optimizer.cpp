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
#include "strategy/join_ordering_rule.hpp"
#include "strategy/join_predicate_ordering_rule.hpp"
#include "strategy/predicate_merge_rule.hpp"
#include "strategy/predicate_placement_rule.hpp"
#include "strategy/predicate_reordering_rule.hpp"
#include "strategy/predicate_split_up_rule.hpp"
#include "strategy/semi_join_reduction_rule.hpp"
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
 *
 * Some optimizer rules affect each other, as noted below. Sometimes, a later rule enables a new optimization for an
 * earlier rule. In the future, it might make sense to bring back iterative groups of rules, but we should keep
 * optimization costs reasonable.
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

  optimizer->add_rule(std::make_unique<ChunkPruningRule>());

  // Run before the JoinOrderingRule so that the latter has simple (non-conjunctive) predicates. However, as the
  // JoinOrderingRule cannot handle UnionNodes (#1829), do not split disjunctions just yet.
  optimizer->add_rule(std::make_unique<PredicateSplitUpRule>(false));

  // The JoinOrderingRule cannot proceed past Semi/Anti Joins. These may be part of the initial query plan (in which
  // case we are out of luck and the join ordering will be sub-optimal) but many of them are also introduced by the
  // SubqueryToJoinRule. As such, we run the JoinOrderingRule before the SubqueryToJoinRule.
  optimizer->add_rule(std::make_unique<JoinOrderingRule>());

  optimizer->add_rule(std::make_unique<BetweenCompositionRule>());

  optimizer->add_rule(std::make_unique<PredicatePlacementRule>());

  optimizer->add_rule(std::make_unique<PredicateSplitUpRule>());

  optimizer->add_rule(std::make_unique<SubqueryToJoinRule>());

  // Run the ColumnPruningRule before the PredicatePlacementRule, as it might turn joins into semi joins, which
  // can be treated as predicates and pushed further down. For the same reason, run it after the JoinOrderingRule,
  // which does not like semi joins (see above).
  optimizer->add_rule(std::make_unique<ColumnPruningRule>());

  optimizer->add_rule(std::make_unique<SemiJoinReductionRule>());

  // Run the PredicatePlacementRule a second time so that semi/anti joins created by the SubqueryToJoinRule and the
  // SemiJoinReductionRule are properly placed, too.
  optimizer->add_rule(std::make_unique<PredicatePlacementRule>());

  optimizer->add_rule(std::make_unique<JoinPredicateOrderingRule>());

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

std::shared_ptr<AbstractLQPNode> Optimizer::optimize(std::shared_ptr<AbstractLQPNode> input) const {
  // We cannot allow multiple owners of the LQP as one owner could decide to optimize the plan and others might hold a
  // pointer to a node that is not even part of the plan anymore after optimization. Thus, callers of this method need
  // to relinquish their ownership (i.e., move their shared_ptr into the method) and take ownership of the resulting
  // optimized plan.
  Assert(input.use_count() == 1, "Optimizer should have exclusive ownership of plan");

  // Add explicit root node, so the rules can freely change the tree below it without having to maintain a root node
  // to return to the Optimizer
  const auto root_node = LogicalPlanRootNode::make(std::move(input));
  input = nullptr;

  if constexpr (HYRISE_DEBUG) validate_lqp(root_node);

  for (const auto& rule : _rules) {
    _apply_rule(*rule, root_node);
    if constexpr (HYRISE_DEBUG) validate_lqp(root_node);
  }

  // Remove LogicalPlanRootNode
  const auto optimized_node = root_node->left_input();
  root_node->set_left_input(nullptr);

  return optimized_node;
}

void Optimizer::validate_lqp(const std::shared_ptr<AbstractLQPNode>& root_node) {
  // If you can think of a way in which an LQP can be corrupt, please add it!

  // First, collect all LQPs (the main LQP and all subqueries)
  auto lqps = std::vector<std::shared_ptr<AbstractLQPNode>>{root_node};
  auto subquery_expressions_by_lqp = SubqueryExpressionsByLQP{};
  auto visited_nodes = std::unordered_set<std::shared_ptr<AbstractLQPNode>>{};
  collect_subquery_expressions_by_lqp(subquery_expressions_by_lqp, root_node, visited_nodes);
  for (const auto& [lqp, subquery_expressions] : subquery_expressions_by_lqp) {
    lqps.emplace_back(lqp);
  }

  std::unordered_map<std::shared_ptr<const AbstractLQPNode>, std::unordered_set<std::shared_ptr<const AbstractLQPNode>>>
      nodes_by_lqp;
  for (const auto& lqp : lqps) {
    visit_lqp(lqp, [&](const auto& node) {
      nodes_by_lqp[lqp].emplace(node);
      return LQPVisitation::VisitInputs;
    });
  }

  // (1) Make sure that all outputs found in an LQP are also part of the same LQP (excluding subqueries)
  // (2) Make sure each node has the number of inputs expected for that node type
  // (3) Make sure that for all LQPColumnExpressions, the original_node is part of the LQP
  for (const auto& lqp : lqps) {
    visit_lqp(lqp, [&](const auto& node) {
      // Check that all outputs are part of the LQP
      const auto outputs = node->outputs();
      for (const auto& output : outputs) {
        // LQP nodes should only be part of a single LQP and all of their outputs should be part of the same LQP. If you
        // run into this assertion as part of a new test, there is a good chance that you are using parts of the LQP
        // outside of the LQP that is currently optimized. For example, if you create a bunch of LQP nodes in the test's
        // SetUp method but these are not used in the current LQP, this can cause this assertion to fail. This is only
        // enforced when rules are executed through the optimizer, not when tests call the rule directly.
        Assert(nodes_by_lqp[lqp].contains(output), std::string{"Output `"} + output->description() + "` of node `" +
                                                       node->description() + "` not found in LQP");

        for (const auto& [other_lqp, nodes] : nodes_by_lqp) {
          if (other_lqp == lqp) continue;
          Assert(!nodes.contains(node), std::string{"Output `"} + output->description() + "` of node `" +
                                            node->description() + "` found in different LQP");
        }
      }

      // Check that all LQPColumnExpressions in the node can be resolved. This mostly guards against expired columns
      // leaving an optimizer rule. It does not guarantee that it the LQPColumnExpressions are correctly returned from
      // one of the (transitive) inputs of `node`. If that is not the case, that will be caught by the LQPTranslator,
      // at the latest. However, feel free to add that check here.
      for (const auto& node_expression : node->node_expressions) {
        visit_expression(node_expression, [&](const auto& sub_expression) {
          if (sub_expression->type != ExpressionType::LQPColumn) return ExpressionVisitation::VisitArguments;
          const auto original_node =
              dynamic_cast<LQPColumnExpression&>(*sub_expression).column_reference.original_node();
          Assert(nodes_by_lqp[lqp].contains(original_node),
                 std::string{"LQPColumnExpression "} + sub_expression->as_column_name() + " can not be resolved");
          return ExpressionVisitation::VisitArguments;
        });
      }

      // Check that the node has the expected number of inputs
      auto num_expected_inputs = size_t{0};
      switch (node->type) {
        case LQPNodeType::CreatePreparedPlan:
        case LQPNodeType::CreateView:
        case LQPNodeType::DummyTable:
        case LQPNodeType::DropView:
        case LQPNodeType::DropTable:
        case LQPNodeType::StaticTable:
        case LQPNodeType::StoredTable:
        case LQPNodeType::Mock:
          num_expected_inputs = 0;
          break;

        case LQPNodeType::Aggregate:
        case LQPNodeType::Alias:
        case LQPNodeType::CreateTable:
        case LQPNodeType::Delete:
        case LQPNodeType::Insert:
        case LQPNodeType::Limit:
        case LQPNodeType::Predicate:
        case LQPNodeType::Projection:
        case LQPNodeType::Root:
        case LQPNodeType::Sort:
        case LQPNodeType::Validate:
          num_expected_inputs = 1;
          break;

        case LQPNodeType::Join:
        case LQPNodeType::Update:
        case LQPNodeType::Union:
          num_expected_inputs = 2;
          break;
      }
      Assert(node->input_count() == num_expected_inputs, std::string{"Node "} + node->description() + " has " +
                                                             std::to_string(node->input_count()) + " inputs, while " +
                                                             std::to_string(num_expected_inputs) + " were expected");

      return LQPVisitation::VisitInputs;
    });
  }
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
    local_root_node->set_left_input(nullptr);
  }
}

}  // namespace opossum
