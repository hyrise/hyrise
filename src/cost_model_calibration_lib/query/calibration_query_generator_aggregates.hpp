#pragma once

#include "logical_query_plan/aggregate_node.hpp"

namespace opossum {

class CalibrationQueryGeneratorAggregates {
 public:
  static const std::shared_ptr<AggregateNode> generate_aggregates();

 private:
  CalibrationQueryGeneratorAggregates() = default;
};

}  // namespace opossum
