#include "abstract_join_ordering_algorithm.hpp"

#include "cost_estimation/abstract_cost_estimator.hpp"
#include "join_graph.hpp"
#include "operators/operator_join_predicate.hpp"
#include "statistics/cardinality_estimator.hpp"

namespace opossum {

std::shared_ptr<AbstractLQPNode> AbstractJoinOrderingAlgorithm::_add_predicates_to_plan(
    const std::shared_ptr<AbstractLQPNode>& lqp, const std::vector<std::shared_ptr<AbstractExpression>>& predicates,
    const std::shared_ptr<AbstractCostEstimator>& cost_estimator) {
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
    std::shared_ptr<AbstractLQPNode> left_lqp, std::shared_ptr<AbstractLQPNode> right_lqp,
    const std::vector<std::shared_ptr<AbstractExpression>>& join_predicates,
    const std::shared_ptr<AbstractCostEstimator>& cost_estimator) {
  /**
   * To make the input sides more deterministic, we make sure that the larger input is on the right side. This helps
   * future rules to identify common subplans. For plans with equal cardinalities, this might still result in
   * non-deterministic orders. Deal with it once it becomes a problem - maybe use the hash?
   */
  if (cost_estimator->cardinality_estimator->estimate_cardinality(left_lqp) <
      cost_estimator->cardinality_estimator->estimate_cardinality(right_lqp)) {
    std::swap(left_lqp, right_lqp);
  }

  /**
   * Join two plans using a set of predicates; try to bring them into an efficient order
   *
   *
   * One predicate ("primary predicate") becomes the join predicate, the others ("secondary predicates") are executed as
   * table scans (column vs column) after the join.
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

  // Categorize join predicates into those that MUST be processed as part of a join operator (because they are required
  // to see if an outer join emits a value or a NULL) and those that can also be processed as regular predicates after
  // the join predicates (post-join predicates, e.g., additional join predicates for inner joins). Since the
  // multi-predicate join is currently slower than a regular predicate (i.e., a full table scan), we only use it in
  // cases where it is required for a correct result.
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
