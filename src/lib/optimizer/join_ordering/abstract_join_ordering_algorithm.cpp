#include "greedy_operator_ordering.hpp"

#include "cost_estimation/abstract_cost_estimator.hpp"
#include "enumerate_ccp.hpp"
#include "join_graph.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "operators/operator_join_predicate.hpp"
#include "statistics/cardinality_estimator.hpp"
#include "utils/format_duration.hpp"
#include "utils/timer.hpp"

namespace opossum {

std::shared_ptr<AbstractLQPNode> AbstractJoinOrderingAlgorithm::_add_predicates_to_plan(
    const std::shared_ptr<AbstractLQPNode>& lqp, const std::vector<std::shared_ptr<AbstractExpression>>& predicates,
    const std::shared_ptr<AbstractCostEstimator>& cost_estimator) const {
  /**
   * Add a number of predicates on top of a plan; try to bring them into an efficient order
   *
   *
   * The optimality-ensuring way to sort the scan operations would be to find the cheapest of the predicates.size()!
   * orders of them.
   * For now, we just execute the scan operations in the order of increasing cost that they would have when executed
   * directly on top of `lqp`
   */

  if (predicates.empty()) return lqp;

  auto predicate_nodes_and_cost = std::vector<std::pair<std::shared_ptr<AbstractLQPNode>, Cost>>{};
  predicate_nodes_and_cost.reserve(predicates.size());
  for (const auto& predicate : predicates) {
    const auto predicate_node = PredicateNode::make(predicate, lqp);
    predicate_nodes_and_cost.emplace_back(predicate_node, cost_estimator->estimate_node_cost(predicate_node));
  }

  std::sort(predicate_nodes_and_cost.begin(), predicate_nodes_and_cost.end(),
            [&](const auto& lhs, const auto& rhs) { return lhs.second < rhs.second; });

  predicate_nodes_and_cost.front().first->set_left_input(lqp);

  for (auto predicate_node_idx = size_t{1}; predicate_node_idx < predicate_nodes_and_cost.size();
       ++predicate_node_idx) {
    predicate_nodes_and_cost[predicate_node_idx].first->set_left_input(
        predicate_nodes_and_cost[predicate_node_idx - 1].first);
  }

  return predicate_nodes_and_cost.back().first;
}

std::shared_ptr<AbstractLQPNode> AbstractJoinOrderingAlgorithm::_add_join_to_plan(
    const std::shared_ptr<AbstractLQPNode>& left_lqp, const std::shared_ptr<AbstractLQPNode>& right_lqp,
    std::vector<std::shared_ptr<AbstractExpression>> join_predicates,
    const std::shared_ptr<AbstractCostEstimator>& cost_estimator) const {
  /**
   * Join two plans using a set of predicates; try to bring them into an efficient order
   *
   *
   * One predicate ("primary predicate") becomes the join predicate, the others ("secondary predicates) are executed as
   * column-to-column scans after the join.
   * The primary predicate needs to be a simple "<column> <operator> <column>" predicate, otherwise the join operators
   * won't be able to execute it.
   *
   * The optimality-ensuring way to order the predicates would be to find the cheapest of the predicates.size()!
   * orders of them.
   * For now, we just execute the scan operations in the order of increasing cost that they would have when executed
   * directly on top of `lqp`, with the cheapest predicate becoming the primary predicate.
   */

  // Sort the predicates by increasing cost
  auto join_predicates_and_cost = std::vector<std::pair<std::shared_ptr<AbstractExpression>, Cost>>{};
  join_predicates_and_cost.reserve(join_predicates.size());
  for (const auto& join_predicate : join_predicates) {
    const auto join_node = JoinNode::make(JoinMode::Inner, join_predicate, left_lqp, right_lqp);
    const auto cost = cost_estimator->estimate_node_cost(join_node);
    join_predicates_and_cost.emplace_back(join_predicate, cost);
  }

  std::sort(join_predicates_and_cost.begin(), join_predicates_and_cost.end(),
            [&](const auto& lhs, const auto& rhs) { return lhs.second < rhs.second; });

  // Categorize join predicates into those that can be processed as part of a join operator and those that need to be
  // processed as normal predicates.
  // NOTE: Since a multi-predicate join is currently slower than scanning the join output table, we do not emit multiple
  //       predicates for the JoinNode, but use subsequent scans instead.
  auto join_node_predicates = std::vector<std::shared_ptr<AbstractExpression>>{};
  auto post_join_node_predicates = std::vector<std::shared_ptr<AbstractExpression>>{};

  for (const auto& [join_predicate, cost] : join_predicates_and_cost) {
    const auto operator_join_predicate = OperatorJoinPredicate::from_expression(*join_predicate, *left_lqp, *right_lqp);
    if (operator_join_predicate && join_node_predicates.empty()) {
      join_node_predicates.emplace_back(join_predicate);
    } else {
      post_join_node_predicates.emplace_back(join_predicate);
    }
  }

  // Build JoinNode (for primary predicate and secondary predicates)
  auto lqp = std::shared_ptr<AbstractLQPNode>{};
  if (!join_node_predicates.empty()) {
    lqp = JoinNode::make(JoinMode::Inner, join_node_predicates, left_lqp, right_lqp);
  } else {
    lqp = JoinNode::make(JoinMode::Cross, left_lqp, right_lqp);
  }

  // Post-JoinNode predicates are handled as normal predicates
  for (const auto& post_join_predicate : post_join_node_predicates) {
    lqp = PredicateNode::make(post_join_predicate, lqp);
  }

  return lqp;
}

}  // namespace opossum
