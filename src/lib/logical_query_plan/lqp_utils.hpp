#pragma once

#include <memory>
#include <optional>
#include <queue>
#include <set>
#include <unordered_set>

#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/alias_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/limit_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/sort_node.hpp"
#include "logical_query_plan/update_node.hpp"

namespace opossum {

class AbstractExpression;
enum class LQPInputSide;

using LQPMismatch = std::pair<std::shared_ptr<const AbstractLQPNode>, std::shared_ptr<const AbstractLQPNode>>;

/**
 * For two equally structured LQPs lhs and rhs, create a mapping for each node in lhs pointing to its equivalent in rhs.
 */
LQPNodeMapping lqp_create_node_mapping(const std::shared_ptr<AbstractLQPNode>& lhs,
                                       const std::shared_ptr<AbstractLQPNode>& rhs);

/**
 * Perform a deep equality check of two LQPs.
 * @return std::nullopt if the LQPs were equal. A pair of a node in this LQP and a node in the rhs LQP that were first
 *         discovered to differ.
 */
std::optional<LQPMismatch> lqp_find_subplan_mismatch(const std::shared_ptr<const AbstractLQPNode>& lhs,
                                                     const std::shared_ptr<const AbstractLQPNode>& rhs);

void lqp_replace_node(const std::shared_ptr<AbstractLQPNode>& original_node,
                      const std::shared_ptr<AbstractLQPNode>& replacement_node);

/**
 * Removes a node from the plan, using the output of its left input as input for its output nodes. Unless
 * allow_right_input is set, the node must not have a right input. If allow_right_input is set, the caller has to
 * retie that right input of the node (or reinsert the node at a different position where the right input is valid).
 */
enum class AllowRightInput { No, Yes };
void lqp_remove_node(const std::shared_ptr<AbstractLQPNode>& node,
                     const AllowRightInput allow_right_input = AllowRightInput::No);

void lqp_insert_node(const std::shared_ptr<AbstractLQPNode>& parent_node, const LQPInputSide input_side,
                     const std::shared_ptr<AbstractLQPNode>& node,
                     const AllowRightInput allow_right_input = AllowRightInput::No);

/**
 * @return whether all paths to all leaves contain a Validate node - i.e. the LQP can be used in an MVCC aware context
 */
bool lqp_is_validated(const std::shared_ptr<AbstractLQPNode>& lqp);

/**
 * @return all names of tables that have been accessed in modifying nodes (e.g., InsertNode, UpdateNode)
 */
std::set<std::string> lqp_find_modified_tables(const std::shared_ptr<AbstractLQPNode>& lqp);

/**
 * Create a boolean expression from an LQP by considering PredicateNodes and UnionNodes. It traverses the LQP from the
 * begin node until it reaches the end node if set or an LQP node which is a not a Predicate, Union, Projection, Sort,
 * Validate or Limit node. The end node is necessary if a certain Predicate should not be part of the created expression
 * 
 * Subsequent Predicate nodes are turned into a LogicalExpression with AND. UnionNodes into a LogicalExpression with OR.
 * Projection, Sort, Validate or Limit LQP nodes are ignored during the traversal.
 *
 *         input LQP   --- lqp_subplan_to_boolean_expression(Sort, Predicate A) --->   boolean expression
 *
 *       Sort (begin node)                                                  Predicate C       Predicate D
 *             |                                                                   \             /
 *           Union                                                                   --- AND ---       Predicate E
 *         /       \                                                                       \              /
 *  Predicate D     |                                                        Predicate B     ---  OR  ---
 *        |      Predicate E                                                        \             /
 *  Predicate C  ´  |                                                                 --- AND ---
 *         \       /                                                                       |
 *        Projection                                                               returned expression
 *             |
 *        Predicate B
 *             |
 *   Predicate A (end node)
 *             |
 *       Stored Table
 *
 * @return      the expression, or nullptr if no expression could be created
 */
std::shared_ptr<AbstractExpression> lqp_subplan_to_boolean_expression(
    const std::shared_ptr<AbstractLQPNode>& begin,
    const std::optional<const std::shared_ptr<AbstractLQPNode>>& end = std::nullopt);

enum class LQPVisitation { VisitInputs, DoNotVisitInputs };

/**
 * Calls the passed @param visitor on @param lqp and recursively on its INPUTS. This will NOT visit subqueries.
 * The visitor returns `LQPVisitation`, indicating whether the current nodes's input should be visited
 * as well. The algorithm is breadth-first search.
 * Each node is visited exactly once.
 *
 * @tparam Visitor      Functor called with every node as a param.
 *                      Returns `LQPVisitation`
 */
template <typename Node, typename Visitor>
void visit_lqp(const std::shared_ptr<Node>& lqp, Visitor visitor) {
  using AbstractNodeType = std::conditional_t<std::is_const_v<Node>, const AbstractLQPNode, AbstractLQPNode>;

  std::queue<std::shared_ptr<AbstractNodeType>> node_queue;
  node_queue.push(lqp);

  std::unordered_set<std::shared_ptr<AbstractNodeType>> visited_nodes;

  while (!node_queue.empty()) {
    auto node = node_queue.front();
    node_queue.pop();

    if (!visited_nodes.emplace(node).second) continue;

    if (visitor(node) == LQPVisitation::VisitInputs) {
      if (node->left_input()) node_queue.push(node->left_input());
      if (node->right_input()) node_queue.push(node->right_input());
    }
  }
}

enum class LQPUpwardVisitation { VisitOutputs, DoNotVisitOutputs };

/**
 * Calls the passed @param visitor on @param lqp and recursively on each node that uses it as an OUTPUT. If the LQP is
 * used as a subquery, the users of the subquery are not visited.
 * The visitor returns `LQPUpwardVisitation`, indicating whether the current nodes's input should be visited
 * as well.
 * Each node is visited exactly once.
 *
 * @tparam Visitor      Functor called with every node as a param.
 *                      Returns `LQPUpwardVisitation`
 */
template <typename Visitor>
void visit_lqp_upwards(const std::shared_ptr<AbstractLQPNode>& lqp, Visitor visitor) {
  std::queue<std::shared_ptr<AbstractLQPNode>> node_queue;
  node_queue.push(lqp);

  std::unordered_set<std::shared_ptr<AbstractLQPNode>> visited_nodes;

  while (!node_queue.empty()) {
    auto node = node_queue.front();
    node_queue.pop();

    if (!visited_nodes.emplace(node).second) continue;

    if (visitor(node) == LQPUpwardVisitation::VisitOutputs) {
      for (const auto& output : node->outputs()) node_queue.push(output);
    }
  }
}

/**
 * @return The node @param lqp as well as the root nodes of all LQPs in subqueries and, recursively, LQPs in their
 *         subqueries
 */
std::vector<std::shared_ptr<AbstractLQPNode>> lqp_find_subplan_roots(const std::shared_ptr<AbstractLQPNode>& lqp);

}  // namespace opossum
