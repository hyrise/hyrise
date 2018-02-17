#include "join_plan_predicate_estimate.hpp"

#include <cmath>

#include "utils/assert.hpp"

namespace opossum {

JoinPlanPredicateEstimate::JoinPlanPredicateEstimate(const float cost,
                                                     const std::shared_ptr<TableStatistics>& statistics)
    : cost(cost), statistics(statistics) {
  DebugAssert(cost >= 0.0f && !std::isnan(cost), "Invalid cost");
}

}  // namespace opossum
