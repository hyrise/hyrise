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

template <ParameterizedLQPCache use_parameterized_cache = ParameterizedLQPCache::Yes, ParameterizedLQPCache = use_parameterized_cache>
class SQLLogicalPlanCache;

template <ParameterizedLQPCache use_parameterized_cache>
class SQLLogicalPlanCache<use_parameterized_cache, ParameterizedLQPCache::Yes> : public GDFSCache<std::shared_ptr<AbstractLQPNode>, std::shared_ptr<ParameterizedPlan>, LQPNodeSharedPtrHash, LQPNodeSharedPtrEqual> {
	using GDFSCache::GDFSCache;
};

template <ParameterizedLQPCache use_parameterized_cache>
class SQLLogicalPlanCache<use_parameterized_cache, ParameterizedLQPCache::No> : public GDFSCache<std::shared_ptr<AbstractLQPNode>, std::shared_ptr<AbstractLQPNode>, LQPNodeSharedPtrHash, LQPNodeSharedPtrEqual> {
	using GDFSCache::GDFSCache;
};

}  // namespace opossum
