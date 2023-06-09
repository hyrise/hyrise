#include "semi_join_reduction_removal_rule.hpp"

#include "cost_estimation/abstract_cost_estimator.hpp"
#include "expression/expression_utils.hpp"
#include "logical_query_plan/join_node.hpp"
#include "semi_join_reduction_rule.hpp"
#include "statistics/cardinality_estimator.hpp"

namespace {

using namespace hyrise;  // NOLINT(build/namespaces)

void remove_semi_join_reductions_recursively(const std::shared_ptr<AbstractLQPNode>& lqp_node,
                                             const std::shared_ptr<AbstractCardinalityEstimator>& cardinality_estimator,
                                             std::unordered_set<std::shared_ptr<AbstractLQPNode>>& visited_nodes) {
  if (!lqp_node || visited_nodes.contains(lqp_node)) {
    return;
  }

  visited_nodes.emplace(lqp_node);

  if (lqp_node->type != LQPNodeType::Join) {
    remove_semi_join_reductions_recursively(lqp_node->left_input(), cardinality_estimator, visited_nodes);
    remove_semi_join_reductions_recursively(lqp_node->right_input(), cardinality_estimator, visited_nodes);
    return;
  }

  const auto& join_node = std::static_pointer_cast<JoinNode>(lqp_node);
  if (!join_node->is_semi_reduction()) {
    remove_semi_join_reductions_recursively(lqp_node->left_input(), cardinality_estimator, visited_nodes);
    remove_semi_join_reductions_recursively(lqp_node->right_input(), cardinality_estimator, visited_nodes);
    return;
  }

  const auto& reduced_node = join_node->get_or_find_reduced_join_node();
  const auto reduced_join_left_input = reduced_node->left_input();
  const auto reduced_join_right_input = reduced_node->right_input();

  // Simple case: reduction still below join.
  if (reduced_join_left_input == join_node || reduced_join_right_input == join_node) {
    const auto cardinality_with_reduction = cardinality_estimator->estimate_cardinality(join_node);
    const auto cardinality_without_reduction = cardinality_estimator->estimate_cardinality(join_node->left_input());

    if (cardinality_with_reduction < cardinality_without_reduction * SemiJoinReductionRule::MINIMUM_SELECTIVITY) {
      join_node->set_right_input(nullptr);
      const auto input_node = join_node->left_input();
      lqp_remove_node(join_node);
      std::cout << "remove " << lqp_node->description() << std::endl;
      remove_semi_join_reductions_recursively(input_node, cardinality_estimator, visited_nodes);
      return;
    }

  } else {
    const auto left_cardinality_with_reduction = cardinality_estimator->estimate_cardinality(reduced_join_left_input);
    const auto right_cardinality_with_reduction = cardinality_estimator->estimate_cardinality(reduced_join_right_input);

    const auto input_node = join_node->left_input();

    lqp_remove_node(join_node, AllowRightInput::Yes);

    const auto left_cardinality_without_reduction =
        cardinality_estimator->estimate_cardinality(reduced_join_left_input);
    const auto right_cardinality_without_reduction =
        cardinality_estimator->estimate_cardinality(reduced_join_right_input);

    if (left_cardinality_with_reduction <
            left_cardinality_without_reduction * SemiJoinReductionRule::MINIMUM_SELECTIVITY ||
        right_cardinality_with_reduction <
            right_cardinality_without_reduction * SemiJoinReductionRule::MINIMUM_SELECTIVITY) {
      join_node->set_right_input(nullptr);
      std::cout << "remove " << lqp_node->description() << std::endl;
      remove_semi_join_reductions_recursively(input_node, cardinality_estimator, visited_nodes);
      return;
    }

    lqp_insert_node_above(input_node, join_node, AllowRightInput::Yes);
  }

  remove_semi_join_reductions_recursively(lqp_node->left_input(), cardinality_estimator, visited_nodes);
  remove_semi_join_reductions_recursively(lqp_node->right_input(), cardinality_estimator, visited_nodes);
  return;
}

}  // namespace

namespace hyrise {

std::string SemiJoinReductionRemovalRule::name() const {
  static const auto name = std::string{"SemiJoinReductionRemovalRule"};
  return name;
}

void SemiJoinReductionRemovalRule::_apply_to_plan_without_subqueries(
    const std::shared_ptr<AbstractLQPNode>& lqp_root) const {
  Assert(lqp_root->type == LQPNodeType::Root, "Rule needs root to hold onto");

  const auto cardinality_estimator = cost_estimator->cardinality_estimator->new_instance();
  auto visited_nodes = std::unordered_set<std::shared_ptr<AbstractLQPNode>>{};

  remove_semi_join_reductions_recursively(lqp_root, cardinality_estimator, visited_nodes);
}

}  // namespace hyrise
