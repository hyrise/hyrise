#pragma once

#include <memory>
#include <string>

#include "cache/cache.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "storage/prepared_plan.hpp"

namespace opossum {

class AbstractOperator;
class AbstractLQPNode;

class LQPNodePointerHash {
 public:
  size_t operator()(const std::shared_ptr<AbstractLQPNode>& key) const { return key->hash(); }
};
class NodePointerDeepEquality {
 public:
  bool operator()(const std::shared_ptr<AbstractLQPNode>& key1, const std::shared_ptr<AbstractLQPNode>& key2) const {
    return *key1 == *key2;
  }
};

using SQLPhysicalPlanCache = Cache<std::shared_ptr<AbstractOperator>, std::string>;
using SQLLogicalPlanCache = Cache<std::shared_ptr<PreparedPlan>, std::shared_ptr<AbstractLQPNode>, LQPNodePointerHash, NodePointerDeepEquality>;
}  // namespace opossum
