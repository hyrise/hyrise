#include "predicate_reordering_rule.hpp"

#include "cost_estimation/abstract_cost_estimator.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "optimizer/join_ordering/join_graph.hpp"
#include "statistics/cardinality_estimation_cache.hpp"
#include "statistics/cardinality_estimator.hpp"
#include "statistics/table_statistics.hpp"
#include "utils/assert.hpp"
#include "utils/timer.hpp"

namespace {
using namespace hyrise;  // NOLINT(build/namespaces)

// Returns whether a certain node is a "predicate-style" node, i.e., a node that can be moved freely within a predicate
// chain.
bool is_predicate_style_node(const std::shared_ptr<AbstractLQPNode>& node) {
  if (node->type == LQPNodeType::Predicate) {
    return true;
  }

  // Validate can be seen as a Predicate on the MVCC column.
  if (node->type == LQPNodeType::Validate) {
    return true;
  }

  // Semi-/anti-joins also reduce the number of tuples and can be freely reordered within a chain of predicates. This
  // might place the join below a ValidateNode, but since it is not a "proper" join (i.e., one that returns columns
  // from multiple tables), the ValidateNode will still be able to operate on the semi-join's output. We do not treat
  // multi-predicate semi- and anti-joins as predicates. Though they also filter the left input, we cannot use shortcuts
  // in the join implementation, which makes them equivalent to regular joins.
  if (node->type == LQPNodeType::Join) {
    const auto& join_node = static_cast<const JoinNode&>(*node);
    if (is_semi_or_anti_join(join_node.join_mode) && join_node.join_predicates().size() == 1) {
      return true;
    }
  }

  return false;
}

// Reorder subsequent predicates based on their estimated logical cost.
void reorder_predicates(const std::vector<std::shared_ptr<AbstractLQPNode>>& predicates,
                        const std::shared_ptr<const AbstractCostEstimator>& cost_estimator,
                        std::chrono::nanoseconds& cardinality_estimator_time) {
  // Store original input and output.
  const auto& input = predicates.back()->left_input();
  const auto outputs = predicates.front()->outputs();
  const auto input_sides = predicates.front()->get_input_sides();

  // Estimate the cardinality of the input node once to cache its estimated statistics. Since we execute the reordering
  // recursively, we can be sure the plan below does not change anymore and the input estimations can be cached safely.
  const auto& cardinality_estimator = cost_estimator->cardinality_estimator;
  auto t1 = Timer{};
  cardinality_estimator->estimate_cardinality(input);
  cardinality_estimator_time += t1.lap();

  // To order the predicates, we want to favor predicates with lower cost over predicates with higher cost. We estimate
  // the cost of each individual predicate on top of the input LQP, i.e., predicates are estimated independently. In the
  // past, we just used the output cardinality. This turned out to be an oversimplification for finding a good order of
  // scans and joins with alike selectivity, where joins are more expensive in general. Cost is logically estimated
  // based on input cardinalities and selectivities of predicates (in fact, it simplifies to summing up scanned input
  // tuples and expected output tuples, see CostEstimatorLogical).
  //
  // We experimented with the following optimization goals:
  //   0) min #out                         (Minimal output cardinality, baseline)
  //   1) max (#in - #out) / (cost - #out) ("Most filtered-out rows per cost")
  //   2) min cost                         (Minimal cost)
  //   3) min #out * cost                  (Minimal output cardinality with cost penalty, also tried +, log, sqrt, ...)
  //   4) min (cost - out) * p + #out      (Cost with a penalty for joins, chosen approach. p = 1.5 for joins, 1 else.)
  //
  // We ended up using 2) the estimated cost to account for the fact that joins are more expensive than predicates.
  // Furthermore, we add a penalty to the input cardinalities since joins always have more overhead than predicates. The
  // factor was derived experimentally, which is far from being a perfect solution, but still better than not
  // incorporating join overhead at all.
  auto nodes_and_costs = std::vector<std::pair<std::shared_ptr<AbstractLQPNode>, Cost>>{};
  nodes_and_costs.reserve(predicates.size());
  for (const auto& predicate : predicates) {
    predicate->set_left_input(input);
    // Estimate the cardinality and cost of the predicate without caching. As the predicate order is not yet determined,
    // caching leads to wrong estimates in the cache.
    constexpr auto do_cache = false;
    auto t2 = Timer{};
    const auto output_cardinality = cardinality_estimator->estimate_cardinality(predicate, do_cache);
    const auto estimated_cost = cost_estimator->estimate_node_cost(predicate, do_cache) - output_cardinality;
    cardinality_estimator_time += t2.lap();
    const auto penalty = predicate->type == LQPNodeType::Join ? PredicateReorderingRule::JOIN_PENALTY : 1.0f;
    const auto weighted_cost = estimated_cost * penalty + output_cardinality;
    nodes_and_costs.emplace_back(predicate, weighted_cost);
  }

  // Untie predicates from LQP, so we can freely retie them.
  for (const auto& predicate : predicates) {
    lqp_remove_node(predicate, AllowRightInput::Yes);
  }

  // Sort in descending order. The "most beneficial" predicate (i.e., with the lowest cost) is at the end.
  std::sort(nodes_and_costs.begin(), nodes_and_costs.end(),
            [&](auto& left, auto& right) { return left.second > right.second; });

  // Ensure that nodes are chained correctly. The predicate at the vector end is placed after the input.
  nodes_and_costs.back().first->set_left_input(input);

  // The predicate at the vector begin is placed before the outputs.
  const auto output_count = outputs.size();
  for (auto output_idx = size_t{0}; output_idx < output_count; ++output_idx) {
    outputs[output_idx]->set_input(input_sides[output_idx], nodes_and_costs.front().first);
  }

  // All predicates are placed as output of (i.e., "after") the next predicate in the vector.
  const auto predicate_count = nodes_and_costs.size() - 1;
  for (auto predicate_index = size_t{0}; predicate_index < predicate_count; ++predicate_index) {
    nodes_and_costs[predicate_index].first->set_left_input(nodes_and_costs[predicate_index + 1].first);
  }
}

// Recursively iterates the LQP, finds chains of subsequent predicates, and reorders these chains.
void reorder_predicates_recursively(const std::shared_ptr<AbstractLQPNode>& node,
                                    const std::shared_ptr<AbstractCostEstimator>& cost_estimator,
                                    std::unordered_set<std::shared_ptr<AbstractLQPNode>>& visited_nodes,
                                    std::chrono::nanoseconds& cardinality_estimator_time) {
  if (!node || visited_nodes.contains(node)) {
    return;
  }

  // Recurse into the inputs if the current node is not a predicate.
  if (!is_predicate_style_node(node)) {
    reorder_predicates_recursively(node->left_input(), cost_estimator, visited_nodes, cardinality_estimator_time);
    reorder_predicates_recursively(node->right_input(), cost_estimator, visited_nodes, cardinality_estimator_time);
    visited_nodes.emplace(node);
    return;
  }

  // Gather adjacent PredicateNodes.
  auto predicate_nodes = std::vector<std::shared_ptr<AbstractLQPNode>>{};
  auto current_node = node;

  while (is_predicate_style_node(current_node)) {
    // Make sure to reorder right input of semi-joins first. Otherwise, we might already cache statistics of predicates
    // we reorder later.
    reorder_predicates_recursively(node->right_input(), cost_estimator, visited_nodes, cardinality_estimator_time);

    // Once a node has multiple outputs, we are not talking about a predicate chain anymore. However, a new chain can
    // start here.
    if (current_node->outputs().size() > 1 && !predicate_nodes.empty()) {
      break;
    }

    predicate_nodes.emplace_back(current_node);
    current_node = current_node->left_input();
  }

  // Recursively reorder predicates below the current predicate chain. Doing so, the plan below the chain does not
  // change after doing that. Thus, the CardinalityEstimator can cache all statistics from nodes below the chain. We
  // used a top-down approach for predicate reordering in the past (`visit_lqp` instead of recursion, starting
  // reordering from the root). That required repeated calls to the CardinalityEstimator without caching, which turned
  // out to be a significant performance bottleneck for short-running queries.
  const auto& deepest_predicate = predicate_nodes.back();
  reorder_predicates_recursively(deepest_predicate->left_input(), cost_estimator, visited_nodes, cardinality_estimator_time);
  reorder_predicates_recursively(deepest_predicate->right_input(), cost_estimator, visited_nodes, cardinality_estimator_time);
  visited_nodes.insert(predicate_nodes.cbegin(), predicate_nodes.cend());

  // A chain of predicates was found. Sort PredicateNodes in descending order with regards to the expected cost.
  if (predicate_nodes.size() > 1) {
    reorder_predicates(predicate_nodes, cost_estimator, cardinality_estimator_time);
  }
}

}  // namespace

namespace hyrise {

std::string PredicateReorderingRule::name() const {
  static const auto name = std::string{"PredicateReorderingRule"};
  return name;
}

void PredicateReorderingRule::_apply_to_plan_without_subqueries(
    const std::shared_ptr<AbstractLQPNode>& lqp_root) const {
  // We reorder recursively from leaves to root. Thus, the CardinalityEstimator may cache already estimated statistics.
  auto t = Timer{};
  auto cardinality_estimator_time = std::chrono::nanoseconds{0};
  const auto caching_cost_estimator = cost_estimator->new_instance();
  caching_cost_estimator->cardinality_estimator->guarantee_bottom_up_construction();

  // We keep track of visited nodes, so that this rule touches nodes once only.
  auto visited_nodes = std::unordered_set<std::shared_ptr<AbstractLQPNode>>{};
  reorder_predicates_recursively(lqp_root, caching_cost_estimator, visited_nodes, cardinality_estimator_time);
  const auto total_time = t.lap();
  const auto& cardinality_estimator =
      static_cast<const CardinalityEstimator&>(*caching_cost_estimator->cardinality_estimator);
  std::cout << "slice " << static_cast<double>(cardinality_estimator.slicing_time.count()) / 1000000.0 << "\nscale "
            << static_cast<double>(cardinality_estimator.scaling_time.count()) / 1000000.0 << "\nestimate "
            << static_cast<double>(cardinality_estimator_time.count()) / 1000000.0 << "\ntotal "
            << static_cast<double>(total_time.count()) / 1000000.0 << "\ncache "
            << cardinality_estimator.cache_time << "\nstore "
            << cardinality_estimator.store_time << "\nscanp "
            << cardinality_estimator.op_scan_time << "\n";

  for(const auto& [type, time] : cardinality_estimator.node_times) {
    std::cout << "node " << magic_enum::enum_name(type) << " " << time << "\n";
  }

  std::cout << std::endl;
}

}  // namespace hyrise
