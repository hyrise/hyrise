#include "calibration_query_generator_aggregates.hpp"

#include "expression/expression_functional.hpp"
#include "logical_query_plan/aggregate_node.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

const std::shared_ptr<AggregateNode> CalibrationQueryGeneratorAggregate::generate_aggregates() {
  return AggregateNode::make(expression_vector(), expression_vector());
}

}  // namespace opossum
