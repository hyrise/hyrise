#include "greedy_operator_ordering.hpp"

#include "cost_model/abstract_cost_estimator.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "operators/operator_join_predicate.hpp"

namespace opossum {

AbstractJoinOrderingAlgorithm::AbstractJoinOrderingAlgorithm(
    const std::shared_ptr<AbstractCostEstimator>& cost_estimator)
    : _cost_estimator(cost_estimator) {}

std::shared_ptr<AbstractLQPNode> AbstractJoinOrderingAlgorithm::_add_predicates_to_plan(
    const std::shared_ptr<AbstractLQPNode>& lqp,
    const std::vector<std::shared_ptr<AbstractExpression>>& predicates) const {
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
    predicate_nodes_and_cost.emplace_back(predicate_node, _cost_estimator->estimate_plan_cost(predicate_node));
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
    const std::vector<std::shared_ptr<AbstractExpression>>& join_predicates) const {
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

  if (join_predicates.empty()) return JoinNode::make(JoinMode::Cross, left_lqp, right_lqp);

  // Sort the predicates by increasing cost
  auto join_predicates_and_cost = std::vector<std::pair<std::shared_ptr<AbstractExpression>, Cost>>{};
  join_predicates_and_cost.reserve(join_predicates.size());
  for (const auto& join_predicate : join_predicates) {
    const auto join_node = JoinNode::make(JoinMode::Inner, join_predicate, left_lqp, right_lqp);
    join_predicates_and_cost.emplace_back(join_predicate, _cost_estimator->estimate_plan_cost(join_node));
  }

  std::sort(join_predicates_and_cost.begin(), join_predicates_and_cost.end(),
            [&](const auto& lhs, const auto& rhs) { return lhs.second < rhs.second; });

  // Find the simple predicate with the lowest cost (if any exists), which will act as the primary predicate
  auto primary_join_predicate = std::shared_ptr<AbstractExpression>{};
  for (auto predicate_iter = join_predicates_and_cost.begin(); predicate_iter != join_predicates_and_cost.end();
       ++predicate_iter) {
    // If a predicate can be converted into an OperatorJoinPredicate, it can be used as a primary predicate
    const auto operator_join_predicate =
        OperatorJoinPredicate::from_expression(*predicate_iter->first, *left_lqp, *right_lqp);
    if (operator_join_predicate) {
      primary_join_predicate = predicate_iter->first;
      join_predicates_and_cost.erase(predicate_iter);
      break;
    }
  }

  // Build JoinNode (for primary predicate) and subsequent scans (for secondary predicates)
  auto lqp = std::shared_ptr<AbstractLQPNode>{};
  if (primary_join_predicate) {
    lqp = JoinNode::make(JoinMode::Inner, primary_join_predicate, left_lqp, right_lqp);
  } else {
    lqp = JoinNode::make(JoinMode::Cross, left_lqp, right_lqp);
  }

  for (const auto& predicate_and_cost : join_predicates_and_cost) {
    lqp = PredicateNode::make(predicate_and_cost.first, lqp);
  }

  return lqp;
}

}  // namespace opossum
