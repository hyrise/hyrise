#pragma once

#include <json.hpp>

#include <string>
#include <vector>

#include "../configuration/calibration_column_specification.hpp"
#include "calibration_query_generator_predicate.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/lqp_column_reference.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/projection_node.hpp"

namespace opossum {

class CalibrationQueryGenerator {
 public:
  CalibrationQueryGenerator(const std::vector<CalibrationColumnSpecification>& column_specifications);

  //    const std::vector<std::shared_ptr<AbstractLQPNode>> CalibrationQueryGenerator::generate_queries() const
  const std::vector<std::shared_ptr<AbstractLQPNode>> generate_queries() const;

 private:
  const std::shared_ptr<AbstractLQPNode> _generate_table_scan(
      const PredicateGeneratorFunctor& predicate_generator) const;
  const std::shared_ptr<AbstractLQPNode> _generate_aggregate() const;
  const std::vector<std::shared_ptr<AbstractLQPNode>> _generate_join() const;

  const std::shared_ptr<ProjectionNode> _generate_projection(const std::vector<LQPColumnReference>& columns) const;
  const std::shared_ptr<MockNode> _column_specification_to_mock_node() const;

  const std::vector<CalibrationColumnSpecification> _column_specifications;
};

}  // namespace opossum
