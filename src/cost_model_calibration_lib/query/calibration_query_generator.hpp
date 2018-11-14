#pragma once

#include <json.hpp>

#include <string>
#include <vector>

#include "../configuration/calibration_column_specification.hpp"
#include "../configuration/calibration_table_specification.hpp"
#include "calibration_query_generator_predicates.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/lqp_column_reference.hpp"
#include "logical_query_plan/projection_node.hpp"

namespace opossum {

class CalibrationQueryGenerator {
 public:
  static const std::vector<std::shared_ptr<AbstractLQPNode>> generate_queries(
      const std::vector<CalibrationTableSpecification>& table_definitions);

 private:
  static const std::shared_ptr<AbstractLQPNode> _generate_table_scan(
      const CalibrationTableSpecification& table_definition, const PredicateGeneratorFunctor& predicate_generator);
  static const std::shared_ptr<AbstractLQPNode> _generate_aggregate(
      const CalibrationTableSpecification& table_definition);
  static const std::vector<std::shared_ptr<AbstractLQPNode>> _generate_join(
      const std::vector<CalibrationTableSpecification>& table_definitions);

  static const std::shared_ptr<ProjectionNode> _generate_projection(const std::vector<LQPColumnReference>& columns);

  CalibrationQueryGenerator() = default;
};

}  // namespace opossum
