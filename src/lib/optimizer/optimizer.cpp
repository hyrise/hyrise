#include "optimizer.hpp"

#include "cost_estimation/cost_estimator_logical.hpp"
#include "expression/expression_utils.hpp"
#include "expression/lqp_subquery_expression.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/logical_plan_root_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "strategy/between_composition_rule.hpp"
#include "strategy/chunk_pruning_rule.hpp"
#include "strategy/column_pruning_rule.hpp"
#include "strategy/dependent_group_by_reduction_rule.hpp"
#include "strategy/expression_reduction_rule.hpp"
#include "strategy/in_expression_rewrite_rule.hpp"
#include "strategy/index_scan_rule.hpp"
#include "strategy/join_ordering_rule.hpp"
#include "strategy/join_predicate_ordering_rule.hpp"
#include "strategy/join_to_predicate_rewrite_rule.hpp"
#include "strategy/join_to_semi_join_rule.hpp"
#include "strategy/null_scan_removal_rule.hpp"
#include "strategy/predicate_merge_rule.hpp"
#include "strategy/predicate_placement_rule.hpp"
#include "strategy/predicate_reordering_rule.hpp"
#include "strategy/predicate_split_up_rule.hpp"
#include "strategy/semi_join_reduction_rule.hpp"
#include "strategy/stored_table_column_alignment_rule.hpp"
#include "strategy/subquery_to_join_rule.hpp"
#include "utils/timer.hpp"

namespace {

using namespace hyrise;  // NOLINT(build/namespaces)

using LQPNodesByLQP = std::unordered_map<std::shared_ptr<const AbstractLQPNode>,
                                         std::unordered_set<std::shared_ptr<const AbstractLQPNode>>>;

std::vector<std::shared_ptr<const AbstractLQPNode>> uncorrelated_subqueries_per_node(
    const std::shared_ptr<const AbstractLQPNode>& node) {
  auto uncorrelated_subqueries = std::vector<std::shared_ptr<const AbstractLQPNode>>{};
  for (const auto& expression : node->node_expressions) {
    visit_expression(expression, [&](const auto& sub_expression) {
      const auto subquery_expression = std::dynamic_pointer_cast<LQPSubqueryExpression>(sub_expression);
      if (subquery_expression && !subquery_expression->is_correlated()) {
        uncorrelated_subqueries.emplace_back(subquery_expression->lqp);
      }

      return ExpressionVisitation::VisitArguments;
    });
  }

  return uncorrelated_subqueries;
}

void assign_node_to_lqp_recursively(const std::shared_ptr<const AbstractLQPNode>& subquery_root_node,
                                    const std::shared_ptr<const AbstractLQPNode>& root_node,
                                    LQPNodesByLQP& nodes_by_lqp) {
  visit_lqp(subquery_root_node, [&](auto& node) {
    nodes_by_lqp[root_node].emplace(node);
    for (const auto& uncorrelated_subquery : uncorrelated_subqueries_per_node(node)) {
      assign_node_to_lqp_recursively(uncorrelated_subquery, root_node, nodes_by_lqp);
    }
    return LQPVisitation::VisitInputs;
  });
}

void validate_lqp_with_uncorrelated_subqueries(const std::shared_ptr<const AbstractLQPNode>& lqp,
                                               const std::shared_ptr<const AbstractLQPNode>& root_lqp,
                                               const LQPNodesByLQP& nodes_by_lqp) {
  // (1) Make sure that all outputs found in an LQP are also part of the same LQP (excluding uncorrelated subqueries).
  // (2) Make sure each node has the number of inputs expected for that node type.
  // (3) Make sure that for all LQPColumnExpressions, the original_node is part of the LQP.
  visit_lqp(lqp, [&](const auto& node) {
    // Check that all outputs are part of the LQP.
    const auto outputs = node->outputs();
    for (const auto& output : outputs) {
      // LQP nodes should only be part of a single LQP and all of their outputs should be part of the same LQP. If you
      // run into this assertion as part of a new test, there is a good chance that you are using parts of the LQP
      // outside of the LQP that is currently optimized. For example, if you create a bunch of LQP nodes in the test's
      // SetUp method but these are not used in the current LQP, this can cause this assertion to fail. This is only
      // enforced when rules are executed through the optimizer, not when tests call the rule directly.
      Assert(nodes_by_lqp.at(root_lqp).contains(output), std::string{"Output `"} + output->description() +
                                                             "` of node `" + node->description() +
                                                             "` not found in LQP.");

      for (const auto& [other_lqp, nodes] : nodes_by_lqp) {
        if (other_lqp == root_lqp) {
          continue;
        }
        Assert(!nodes.contains(node), std::string{"Output `"} + output->description() + "` of node `" +
                                          node->description() + "` found in different LQP.");
      }
    }

    // Check that all LQPColumnExpressions in the node can be resolved. This mostly guards against expired columns
    // leaving an optimizer rule. It does not guarantee that it the LQPColumnExpressions are correctly returned from one
    // of the (transitive) inputs of `node`. If that is not the case, that will be caught by the LQPTranslator, at the
    // latest. However, feel free to add that check here.
    for (const auto& node_expression : node->node_expressions) {
      visit_expression(node_expression, [&](const auto& sub_expression) {
        if (sub_expression->type != ExpressionType::LQPColumn) {
          return ExpressionVisitation::VisitArguments;
        }

        const auto original_node = static_cast<LQPColumnExpression&>(*sub_expression).original_node.lock();
        Assert(original_node, "LQPColumnExpression is expired, LQP is invalid.");
        Assert(nodes_by_lqp.at(root_lqp).contains(original_node),
               std::string{"LQPColumnExpression "} + sub_expression->as_column_name() + " cannot be resolved.");
        return ExpressionVisitation::VisitArguments;
      });
    }

    // Check that the node has the expected number of inputs.
    auto num_expected_inputs = size_t{0};
    switch (node->type) {
      case LQPNodeType::CreatePreparedPlan:
      case LQPNodeType::CreateView:
      case LQPNodeType::DummyTable:
      case LQPNodeType::DropView:
      case LQPNodeType::DropTable:
      case LQPNodeType::Import:
      case LQPNodeType::StaticTable:
      case LQPNodeType::StoredTable:
      case LQPNodeType::Mock:
        num_expected_inputs = 0;
        break;

      case LQPNodeType::Aggregate:
      case LQPNodeType::Alias:
      case LQPNodeType::CreateTable:
      case LQPNodeType::Delete:
      case LQPNodeType::Export:
      case LQPNodeType::Insert:
      case LQPNodeType::Limit:
      case LQPNodeType::Predicate:
      case LQPNodeType::Projection:
      case LQPNodeType::Root:
      case LQPNodeType::Sort:
      case LQPNodeType::Validate:
      case LQPNodeType::Window:
        num_expected_inputs = 1;
        break;

      case LQPNodeType::Join:
      case LQPNodeType::ChangeMetaTable:
      case LQPNodeType::Update:
      case LQPNodeType::Union:
      case LQPNodeType::Intersect:
      case LQPNodeType::Except:
        num_expected_inputs = 2;
        break;
    }
    Assert(node->input_count() == num_expected_inputs, std::string{"Node "} + node->description() + " has " +
                                                           std::to_string(node->input_count()) + " inputs, while " +
                                                           std::to_string(num_expected_inputs) + " were expected.");

    for (const auto& uncorrelated_subquery : uncorrelated_subqueries_per_node(node)) {
      validate_lqp_with_uncorrelated_subqueries(uncorrelated_subquery, root_lqp, nodes_by_lqp);
    }

    return LQPVisitation::VisitInputs;
  });
}

}  // namespace

namespace hyrise {

OptimizerRuleMetrics::OptimizerRuleMetrics(const std::string& init_rule_name,
                                           const std::chrono::nanoseconds init_duration)
    : rule_name{init_rule_name}, duration{init_duration} {}

/**
 * Some optimizer rules affect each other, as noted below. Sometimes, a later rule enables a new optimization for an
 * earlier rule. In the future, it might make sense to bring back iterative groups of rules, but we should keep
 * optimization costs reasonable.
 */
std::shared_ptr<Optimizer> Optimizer::create_default_optimizer() {
  auto optimizer = std::make_shared<Optimizer>();

  optimizer->add_rule(std::make_unique<ExpressionReductionRule>());

  // Run before the JoinOrderingRule so that the latter has simple (non-conjunctive) predicates. However, as the
  // JoinOrderingRule cannot handle UnionNodes (#1829), do not split disjunctions just yet.
  optimizer->add_rule(std::make_unique<PredicateSplitUpRule>(false));

  // The JoinOrderingRule cannot proceed past Semi/Anti Joins. These may be part of the initial query plan (in which
  // case we are out of luck and the join ordering will be sub-optimal) but many of them are also introduced by the
  // SubqueryToJoinRule. As such, we run the JoinOrderingRule before the SubqueryToJoinRule.
  optimizer->add_rule(std::make_unique<JoinOrderingRule>());

  // Run Group-By Reduction after the JoinOrderingRule ran. The actual join order is not important, but the matching
  // of cross joins with predicates that is done by that rule is needed to create some of the functional dependencies
  // (FDs) used by the DependentGroupByReductionRule.
  optimizer->add_rule(std::make_unique<DependentGroupByReductionRule>());

  optimizer->add_rule(std::make_unique<BetweenCompositionRule>());

  optimizer->add_rule(std::make_unique<PredicatePlacementRule>());

  optimizer->add_rule(std::make_unique<PredicateSplitUpRule>());

  optimizer->add_rule(std::make_unique<NullScanRemovalRule>());

  optimizer->add_rule(std::make_unique<SubqueryToJoinRule>());

  optimizer->add_rule(std::make_unique<ColumnPruningRule>());

  // Run the JoinToSemiJoinRule and the JoinToPredicateRewriteRule before the PredicatePlacementRule, as they might turn
  // joins into semi joins (which are treated as predicates) or predicates that can be pushed further down. For the same
  // reason, run them after the JoinOrderingRule, which does not like semi joins (see above). Furthermore, these two
  // rules depend on the ColumnPruningRule that flags joins where one input is not used later in the query plan.
  optimizer->add_rule(std::make_unique<JoinToSemiJoinRule>());

  optimizer->add_rule(std::make_unique<JoinToPredicateRewriteRule>());

  // The SemiJoinReductionRule is very sensitive to the predicate placement and order present when it is applied. In
  // general, running the PredicatePlacementRule and the PredicateReorderingRule before the SemiJoinReductionRule is
  // beneficial. However, TPC-H Q 21 (that is already long-running) degrades drastically. See:
  // https://github.com/hyrise/hyrise/pull/2536#issuecomment-1423076256
  // TODO(anyone): Re-evaluate this in the future.
  optimizer->add_rule(std::make_unique<SemiJoinReductionRule>());

  // Run the PredicatePlacementRule a second time so that semi/anti joins created by the SubqueryToJoinRule, the
  // JoinToSemiJoinRule, and the SemiJoinReductionRule, or predicates created by the JoinToPredicateRewriteRule are
  // properly placed, too.
  optimizer->add_rule(std::make_unique<PredicatePlacementRule>());

  optimizer->add_rule(std::make_unique<JoinPredicateOrderingRule>());

  // Prune chunks after the BetweenCompositionRule ran, as `a >= 5 AND a <= 7` may not be prunable predicates while
  // `a BETWEEN 5 and 7` is. Also, run it after the PredicatePlacementRule, so that predicates are as close to the
  // StoredTableNode as possible where the ChunkPruningRule can work with them.
  optimizer->add_rule(std::make_unique<ChunkPruningRule>());

  // The LQPTranslator may translate two individual but equivalent LQP nodes into the same PQP operator. The
  // StoredTableColumnAlignmentRule supports this effort by aligning the list of pruned column ids across nodes that
  // could become deduplicated. For this, the ColumnPruningRule needs to have been executed.
  optimizer->add_rule(std::make_unique<StoredTableColumnAlignmentRule>());

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

std::shared_ptr<AbstractLQPNode> Optimizer::optimize(
    std::shared_ptr<AbstractLQPNode> input,
    const std::shared_ptr<std::vector<OptimizerRuleMetrics>>& rule_durations) const {
  // We cannot allow multiple owners of the LQP as one owner could decide to optimize the plan and others might hold a
  // pointer to a node that is not even part of the plan anymore after optimization. Thus, callers of this method need
  // to relinquish their ownership (i.e., move their shared_ptr into the method) and take ownership of the resulting
  // optimized plan.
  Assert(input.use_count() == 1, "Optimizer should have exclusive ownership of plan.");

  // Add explicit root node, so the rules can freely change the tree below it without having to maintain a root node
  // to return to the Optimizer
  const auto root_node = LogicalPlanRootNode::make(std::move(input));
  input = nullptr;

  if constexpr (HYRISE_DEBUG) {
    validate_lqp(root_node);
  }

  for (const auto& rule : _rules) {
    auto rule_timer = Timer{};
    rule->apply_to_plan(root_node);

    if (rule_durations) {
      rule_durations->emplace_back(rule->name(), rule_timer.lap());
    }

    if constexpr (HYRISE_DEBUG) {
      validate_lqp(root_node);
    }
  }

  // Remove LogicalPlanRootNode.
  auto optimized_node = root_node->left_input();
  root_node->set_left_input(nullptr);

  return optimized_node;
}

void Optimizer::validate_lqp(const std::shared_ptr<AbstractLQPNode>& root_node) {
  // If you can think of a way in which an LQP can be corrupt, please add it!
  // First, collect all LQPs (the main LQP and all uncorrelated subqueries).
  auto lqps = std::vector<std::shared_ptr<AbstractLQPNode>>{root_node};
  for (const auto& [lqp, _] : collect_lqp_subquery_expressions_by_lqp(root_node, true)) {
    lqps.emplace_back(lqp);
  }

  auto nodes_by_lqp = LQPNodesByLQP{};
  // Second, assign each LQPNode to its LQP. Since the results of uncorrelated subquery LQPs do not depend on correlated
  // parameters, we can use them in multiple LQPSubqueryExpressions and treat them like normal LQPNodes. However,
  // correlated subqueries must not share nodes with any other (sub-)LQP. Correlated subquery LQPs must not be reused,
  // they are completely managed by the ExpressionEvaluator, which deep copies them for each row.
  for (const auto& lqp : lqps) {
    assign_node_to_lqp_recursively(lqp, lqp, nodes_by_lqp);
  }

  // Third, check that each of the LQPs is valid.
  for (const auto& lqp : lqps) {
    validate_lqp_with_uncorrelated_subqueries(lqp, lqp, nodes_by_lqp);
  }
}

}  // namespace hyrise
