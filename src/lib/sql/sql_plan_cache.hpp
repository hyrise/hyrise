#pragma once

#include <memory>
#include <string>

#include "cache/gdfs_cache.hpp"
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

using SQLPhysicalPlanCache = GDFSCache<std::string, std::shared_ptr<AbstractOperator>>;
using SQLLogicalPlanCache = GDFSCache<std::shared_ptr<AbstractLQPNode>, std::shared_ptr<PreparedPlan>,
                                      LQPNodePointerHash, NodePointerDeepEquality>;

}  // namespace opossum
