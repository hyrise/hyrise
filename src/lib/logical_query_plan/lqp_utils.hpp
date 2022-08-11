#pragma once

#include <memory>
#include <optional>
#include <queue>
#include <set>
#include <unordered_set>
#include <utility>
#include <vector>

#include "logical_query_plan/abstract_lqp_node.hpp"

namespace hyrise {

class AbstractExpression;
class AbstractLQPNode;
class LQPSubqueryExpression;

enum class LQPInputSide;

using LQPMismatch = std::pair<std::shared_ptr<const AbstractLQPNode>, std::shared_ptr<const AbstractLQPNode>>;

/**
 * Data structure that maps LQPs to (multiple) subquery expressions that reference them.
 *
 * Purpose:
 *  Mainly used by optimizer rules to optimize subquery LQPs more efficiently. In concrete, it helps
 *  to optimize subquery LQPs ONLY ONCE, although being referenced by a list of subquery expressions.
 *
 * Why weak pointers for subquery expressions?
 *  Referenced LQPs and subquery expressions might be subject to change after creating this data structure. Depending
 *  on the order of optimization steps, we could end up with a scenario as follows:
 *
 *      [ProjectionNodeRoot(..., SubqueryExpressionA)]
 *                                        \
 *                                         \ references
 *                                          \
 *                                        [ProjectionNodeA(..., SubqueryExpressionB)]
 *                                                                       \
 *                                                                        \ references
 *                                                                         \
 *                                                                        [ProjectionNodeB(...)]
 *
 *        (1) collect_lqp_subquery_expressions_by_lqp(ProjectionNodeRoot)
 *                =>  returns { [ ProjectionNodeA, { SubqueryExpressionA } ],
 *                              [ ProjectionNodeB, { SubqueryExpressionB } ] }
 *
 *        (2) OptimizerRuleXY is applied to ProjectionNodeA
 *                => As a result, SubqueryExpressionB gets replaced / removed from ProjectionNodeA.
 *
 *        (3) OptimizerRuleXY is applied to ProjectionNodeB
 *            -> Wasted optimization time because SubqueryExpressionB and ProjectionNodeB are no longer being used,
 *               thanks to step (2).
 *
 *  With weak pointers, we are forced to skip step (3) because SubqueryExpressionB and its corresponding LQP have
 *  already been deleted after step (2).
 *
 *  However, this optimization does not cover all cases as it is dependent on the execution order. For
 *  example, when swapping steps (2) and (3), we cannot easily skip an optimization step.
 */
using SubqueryExpressionsByLQP =
    std::unordered_map<std::shared_ptr<AbstractLQPNode>, std::vector<std::weak_ptr<LQPSubqueryExpression>>>;

/**
 * Returns unique LQPs from (nested) LQPSubqueryExpressions of @param node.
 */
SubqueryExpressionsByLQP collect_lqp_subquery_expressions_by_lqp(const std::shared_ptr<AbstractLQPNode>& node);

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
                     const std::shared_ptr<AbstractLQPNode>& node_to_insert,
                     const AllowRightInput allow_right_input = AllowRightInput::No);

/**
 * Sets @param node as the left input of @param node_to_insert, and re-connects all outputs to @param node_to_insert.
 */
void lqp_insert_node_above(const std::shared_ptr<AbstractLQPNode>& node,
                           const std::shared_ptr<AbstractLQPNode>& node_to_insert,
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
 * Subsequent PredicateNodes are turned into a LogicalExpression with AND. UnionNodes into a LogicalExpression with OR.
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
 *  Predicate C     |                                                                 --- AND ---
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

    if (!visited_nodes.emplace(node).second) {
      continue;
    }

    if (visitor(node) == LQPVisitation::VisitInputs) {
      if (node->left_input()) {
        node_queue.push(node->left_input());
      }
      if (node->right_input()) {
        node_queue.push(node->right_input());
      }
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

    if (!visited_nodes.emplace(node).second) {
      continue;
    }

    if (visitor(node) == LQPUpwardVisitation::VisitOutputs) {
      for (const auto& output : node->outputs()) {
        node_queue.push(output);
      }
    }
  }
}

/**
 * @return The node @param lqp as well as the root nodes of all LQPs in subqueries and, recursively, LQPs in their
 *         subqueries
 */
std::vector<std::shared_ptr<AbstractLQPNode>> lqp_find_subplan_roots(const std::shared_ptr<AbstractLQPNode>& lqp);

/**
 * Traverses @param lqp from the top to the bottom and returns all nodes of the given @param type.
 */
std::vector<std::shared_ptr<AbstractLQPNode>> lqp_find_nodes_by_type(const std::shared_ptr<AbstractLQPNode>& lqp,
                                                                     const LQPNodeType type);
/**
 * Traverses @param lqp from the top to the bottom and @returns all leaf nodes.
 */
std::vector<std::shared_ptr<AbstractLQPNode>> lqp_find_leaves(const std::shared_ptr<AbstractLQPNode>& lqp);

/**
 * @return A set of column expressions created by the given @param lqp_node, matching the given @param column_ids.
 *         This is a helper method that maps column ids from tables to the matching output expressions. Conceptually,
 *         it only works on data source nodes. Currently, these are StoredTableNodes, StaticTableNodes and MockNodes.
 */
ExpressionUnorderedSet find_column_expressions(const AbstractLQPNode& lqp_node, const std::set<ColumnID>& column_ids);

/**
 * @return True, if there is unique constraint in the given set of @param unique_constraints matching the given
 *         set of expressions. A unique constraint matches if it covers a subset of @param expressions.
 */
bool contains_matching_unique_constraint(const std::shared_ptr<LQPUniqueConstraints>& unique_constraints,
                                         const ExpressionUnorderedSet& expressions);

/**
 * @return A set of FDs, derived from the given @param unique_constraints and based on the output expressions of the
 *         given @param lqp node.
 */
std::vector<FunctionalDependency> fds_from_unique_constraints(
    const std::shared_ptr<const AbstractLQPNode>& lqp, const std::shared_ptr<LQPUniqueConstraints>& unique_constraints);

/**
 * This is a helper method that removes invalid or unnecessary FDs from the given input set @param fds by looking at
 * the @param lqp node's output expressions.
 */
void remove_invalid_fds(const std::shared_ptr<const AbstractLQPNode>& lqp, std::vector<FunctionalDependency>& fds);

/**
 * Takes the given UnionNode @param union_root_node and traverses the LQP until a common origin node was found.
 * @returns a shared pointer to the diamond's origin node. If it was not found, a null pointer is returned.
 */
std::shared_ptr<AbstractLQPNode> find_diamond_origin_node(const std::shared_ptr<AbstractLQPNode>& union_root_node);

}  // namespace hyrise
