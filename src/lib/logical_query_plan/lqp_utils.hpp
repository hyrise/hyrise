#pragma once

#include <memory>
#include <unordered_map>
#include <optional>

namespace opossum {

class AbstractLQPNode;

using LQPNodeMapping = std::unordered_map<std::shared_ptr<const AbstractLQPNode>, std::shared_ptr<AbstractLQPNode>>;
using LQPMismatch = std::pair<std::shared_ptr<const AbstractLQPNode>, std::shared_ptr<const AbstractLQPNode>>;

/**
 * For two equally structured LQPs lhs and rhs, create a mapping for each node in lhs pointing to its equivalent in rhs.
 */
LQPNodeMapping lqp_create_node_mapping(
  const std::shared_ptr<AbstractLQPNode>& lhs, const std::shared_ptr<AbstractLQPNode>& rhs
);

/**
 * Perform a deep equality check of two LQPs.
 * @return std::nullopt if the LQPs were equal. A pair of a node in this LQP and a node in the rhs LQP that were first
 *         discovered to differ.
 */
std::optional<LQPMismatch> lqp_find_subplan_mismatch(const std::shared_ptr<AbstractLQPNode>& lhs, const std::shared_ptr<AbstractLQPNode>& rhs);


}  // namespace opossum
