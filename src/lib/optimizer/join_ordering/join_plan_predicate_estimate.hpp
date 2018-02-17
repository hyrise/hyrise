#pragma once

#include <memory>

namespace opossum {

class TableStatistics;

class JoinPlanPredicateEstimate {
 public:
  JoinPlanPredicateEstimate(const float cost, const std::shared_ptr<TableStatistics>& statistics);

  const float cost;
  std::shared_ptr<TableStatistics> statistics;
};

}  // namespace opossum
