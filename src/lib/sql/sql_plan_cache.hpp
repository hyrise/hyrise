#pragma once

#include <memory>
#include <string>

#include "cache/cache.hpp"
#include "cache/gdfs_cache.hpp"
#include "cache/gdfs_cache_lock.hpp"

namespace opossum {

class AbstractOperator;
class AbstractLQPNode;

using SQLPhysicalPlanCacheOld = Cache<std::shared_ptr<AbstractOperator>, std::string>;
using SQLLogicalPlanCacheOld = Cache<std::shared_ptr<AbstractLQPNode>, std::string>;

using SQLPhysicalPlanCache = GDFSCache<std::string, std::shared_ptr<AbstractOperator>>;
using SQLLogicalPlanCache = GDFSCache<std::string, std::shared_ptr<AbstractLQPNode>>;

using SQLPhysicalPlanCacheLock = GDFSCacheLock<std::string, std::shared_ptr<AbstractOperator>>;
using SQLLogicalPlanCacheLock = GDFSCacheLock<std::string, std::shared_ptr<AbstractLQPNode>>;

}  // namespace opossum
