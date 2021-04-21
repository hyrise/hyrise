#pragma once

#include <memory>
#include <string>

#include "cache/gdfs_cache.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "storage/prepared_plan.hpp"

namespace opossum {

class AbstractOperator;
class AbstractLQPNode;

using ParameterizedPlan = PreparedPlan;

using SQLPhysicalPlanCache = GDFSCache<std::string, std::shared_ptr<AbstractOperator>>;

class SQLLogicalPlanCache : public GDFSCache<std::shared_ptr<AbstractLQPNode>, std::shared_ptr<ParameterizedPlan>,
                                             LQPNodeSharedPtrHash, LQPNodeSharedPtrEqual> {
 public:
  explicit SQLLogicalPlanCache(ParameterizedLQPCache use_parameterized_cache_init = ParameterizedLQPCache::No)
      : use_parameterized_cache(use_parameterized_cache_init){}

  ParameterizedLQPCache use_parameterized_cache;
};

}  // namespace opossum
