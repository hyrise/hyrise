#pragma once

#include <memory>
#include <unordered_map>

namespace opossum {

class AbstractLQPNode;

using LQPNodeMapping = std::unordered_map<std::shared_ptr<const AbstractLQPNode>, std::shared_ptr<AbstractLQPNode>>;

/**
 * For two equally structured LQPs lhs and rhs, create a mapping for each node in lhs pointing to its equivalent in rhs.
 */
LQPNodeMapping lqp_create_node_mapping(
  const std::shared_ptr<AbstractLQPNode>& lhs, const std::shared_ptr<AbstractLQPNode>& rhs
);

}  // namespace opossum
