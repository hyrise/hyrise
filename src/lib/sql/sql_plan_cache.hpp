#pragma once

#include <memory>
#include <string>

#include "cache/gdfs_cache.hpp"

namespace hyrise {

class AbstractOperator;
class AbstractLQPNode;

using SQLPhysicalPlanCache = GDFSCache<std::string, std::shared_ptr<AbstractOperator>>;
using SQLLogicalPlanCache = GDFSCache<std::string, std::shared_ptr<AbstractLQPNode>>;

}  // namespace hyrise
