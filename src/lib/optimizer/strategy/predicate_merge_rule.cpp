#include "predicate_merge_rule.hpp"

#include "expression/expression_functional.hpp"
#include "expression/logical_expression.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/union_node.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

/**
 * Merge subplans that only consists of PredicateNodes and UnionNodes (with UnionMode::Positions) into a single
 * PredicateNode. A subplan consists of linear "chain" and forked "diamond" parts.
 *
 * EXAMPLE:
 *
 *           |                                       |                                       |
 *      ___Union___                           Predicate(a OR b)            Predicate((d AND c) AND (a OR b))
 *    /            \                                 |                                       |
 * Predicate(a)    |                                 |                                       |
 *    |            |                                 |                                       |
 *    |       Predicate(b)                           |                                       |
 *    \           /               ----->             |                ----->                 |
 *     Predicate(c)                             Predicate(c)                                 |
 *           |                                       |                                       |
 *     Predicate(d)                             Predicate(d)                                 |
 *           |                                       |                                       |
 *         Table                                   Table                                   Table
 *
 * Note that predicates c and d are now also included in the new predicate and will be evaluated by the
 * ExpressionEvaluator. The reason for this is that the current detection algorithm does not yet identify predicates
 * that are inputs to a merged subplan but do not necessarily belong to that subplan. When it becomes necessary, this
 * rule might be adapted to make more sophisticated decisions on which predicates to include.
 */
void PredicateMergeRule::apply_to(const std::shared_ptr<AbstractLQPNode>& root) const {
  Assert(root->type == LQPNodeType::Root, "PredicateMergeRule needs root to hold onto");

  // (Potentially mergeable) subplans are identified by their topmost UnionNode. node_to_topmost holds a mapping from
  // PredicateNodes and UnionNodes within such subplans to the respective topmost UnionNode.
  std::map<const std::shared_ptr<AbstractLQPNode>, const std::shared_ptr<UnionNode>> node_to_topmost;

  // Store the same nodes as node_to_topmost but preserve their order
  std::queue<std::shared_ptr<AbstractLQPNode>> node_queue;

  // For every topmost UnionNode, store the total number of UnionNodes in its subplan below
  std::map<const std::shared_ptr<UnionNode>, size_t> topmost_to_union_count;

  // Step 1: Collect mergeable nodes
  visit_lqp(root, [&](const auto& node) {
    const auto& union_node = std::dynamic_pointer_cast<UnionNode>(node);
    // We need to check for UnionMode::Positions as other UnionModes are not guaranteed to represent a disjunction
    if (node->type == LQPNodeType::Predicate || (union_node && union_node->union_mode == UnionMode::Positions)) {
      const auto& outputs = node->outputs();
      const auto parent = std::find_if(outputs.begin(), outputs.end(),
                                       [&](const auto& output) { return node_to_topmost.count(output); });

      if (parent == outputs.end() && union_node) {
        // New subplan found
        node_to_topmost.emplace(union_node, union_node);
        node_queue.push(union_node);
        topmost_to_union_count.emplace(union_node, 0);
      } else if (parent != outputs.end()) {
        // New node for a known subplan found
        node_to_topmost.emplace(node, node_to_topmost[*parent]);
        node_queue.push(node);
      }

      if (union_node) {
        topmost_to_union_count[node_to_topmost[node]]++;
      }
    }
    return LQPVisitation::VisitInputs;
  });

  // Step 2: Try to merge each node
  while (!node_queue.empty()) {
    const auto& node = node_queue.front();
    node_queue.pop();

    if (node->output_count() == 0 || topmost_to_union_count[node_to_topmost[node]] < minimum_union_count) {
      // The node was already removed due to a previous merge or its subplan is too small to be merged.
      continue;
    }

    switch (node->type) {
      // _merge_disjunction() and _merge_conjunction() merge UnionNodes and PredicateNodes by
      //   a) being called on any node inside a mergable subplan
      //   b) calling each other recursively after a merge to check whether they can merge right again
      case LQPNodeType::Union: {
        const auto& union_node = std::static_pointer_cast<UnionNode>(node);
        _merge_disjunction(union_node);
        break;
      }

      case LQPNodeType::Predicate: {
        _merge_conjunction(std::static_pointer_cast<PredicateNode>(node));
        break;
      }

      default: {}
    }
  }
}

/**
 * Merge "simple" diamonds, which only consist of one UnionNode, having two PredicateNodes as inputs
 */
void PredicateMergeRule::_merge_disjunction(const std::shared_ptr<UnionNode>& union_node) const {
  Assert(union_node->union_mode == UnionMode::Positions, "Cannot merge union_node into disjunction");
  const auto left_predicate_node = std::dynamic_pointer_cast<PredicateNode>(union_node->left_input());
  const auto right_predicate_node = std::dynamic_pointer_cast<PredicateNode>(union_node->right_input());

  // Step 1: Check if the diamond is simple
  if (!left_predicate_node || !right_predicate_node ||
      left_predicate_node->left_input() != right_predicate_node->left_input() ||
      left_predicate_node->output_count() != 1 || right_predicate_node->output_count() != 1) {
    return;
  }

  // Step 2: Merge diamond
  const auto merged_predicate = or_(left_predicate_node->predicate(), right_predicate_node->predicate());
  const auto merged_predicate_node = PredicateNode::make(merged_predicate);
  lqp_remove_node(left_predicate_node);
  lqp_remove_node(right_predicate_node);
  union_node->set_right_input(nullptr);
  lqp_replace_node(union_node, merged_predicate_node);

  // Step 3: Try to merge other nodes
  // There could be another diamond that just became simple so that it can be merged.
  const auto parent_union_node = std::dynamic_pointer_cast<UnionNode>(merged_predicate_node->outputs().front());
  if (parent_union_node && parent_union_node->union_mode == UnionMode::Positions) {
    _merge_disjunction(parent_union_node);
  }

  if (merged_predicate_node->output_count()) {
    // There was no disjunction above that could be merged. But there could be a predicate chain that just became simple
    // so that it can be merged.
    const auto parent_predicate_node =
        std::dynamic_pointer_cast<PredicateNode>(merged_predicate_node->outputs().front());
    if (parent_predicate_node) {
      _merge_conjunction(parent_predicate_node);
    } else {
      _merge_conjunction(merged_predicate_node);
    }
  }
}

/**
 * Merge "simple" predicate chains, which only consist of PredicateNodes
 */
void PredicateMergeRule::_merge_conjunction(const std::shared_ptr<PredicateNode>& predicate_node) const {
  std::vector<std::shared_ptr<PredicateNode>> predicate_nodes;

  // Step 1: Collect other PredicateNodes in predicate chain
  std::shared_ptr<AbstractLQPNode> current_node = predicate_node->left_input();
  while (current_node->type == LQPNodeType::Predicate) {
    // Once a node has multiple outputs, we're not talking about a predicate chain anymore.
    if (current_node->outputs().size() > 1) {
      break;
    }
    predicate_nodes.emplace_back(std::static_pointer_cast<PredicateNode>(current_node));
    current_node = current_node->left_input();
  }

  if (predicate_nodes.empty()) {
    return;
  }

  // Step 2: Merge predicate chain
  auto merged_predicate = predicate_node->predicate();
  for (const auto& current_predicate_node : predicate_nodes) {
    merged_predicate = and_(current_predicate_node->predicate(), merged_predicate);
    lqp_remove_node(current_predicate_node);
  }
  const auto merged_predicate_node = PredicateNode::make(merged_predicate);
  lqp_replace_node(predicate_node, merged_predicate_node);

  // Step 3: Try to merge other nodes
  // There could be a diamond that just became simple so that it can be merged.
  const auto parent_union_node = std::dynamic_pointer_cast<UnionNode>(merged_predicate_node->outputs().front());
  if (parent_union_node) _merge_disjunction(parent_union_node);
}

}  // namespace opossum
