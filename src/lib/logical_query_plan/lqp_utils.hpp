#pragma once

#include <memory>
#include <optional>
#include <unordered_map>

namespace opossum {

class AbstractLQPNode;
class AbstractExpression;

using LQPNodeMapping = std::unordered_map<std::shared_ptr<const AbstractLQPNode>, std::shared_ptr<AbstractLQPNode>>;
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

void lqp_remove_node(const std::shared_ptr<AbstractLQPNode>& node);

/**
 * @return whether all paths to all leafs contain a Validate node - i.e. the LQP can be used in an MVCC aware context
 */
bool lqp_is_validated(const std::shared_ptr<AbstractLQPNode>& lqp);

/**
 * Create a boolean expression from an LQP by considering PredicateNodes and UnionNodes
 * @return      the expression, or nullptr if no expression could be created
 */
std::shared_ptr<AbstractExpression> lqp_subplan_to_boolean_expression(const std::shared_ptr<AbstractLQPNode>& lqp);

}  // namespace opossum
