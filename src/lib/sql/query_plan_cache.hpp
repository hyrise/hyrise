#pragma once

#include <memory>
#include <string>

#include "cache/hash_cache.hpp"

namespace opossum {

class AbstractOperator;

using QueryPlanCache = HashCache<std::shared_ptr<AbstractOperator>, std::string>;

}  // namespace opossum
