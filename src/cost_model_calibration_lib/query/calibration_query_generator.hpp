#pragma once

#include <nlohmann/json.hpp>

#include <string>
#include <vector>

#include "../configuration/calibration_column_specification.hpp"
#include "../configuration/calibration_configuration.hpp"
#include "calibration_query_generator_join.hpp"
#include "calibration_query_generator_predicate.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/projection_node.hpp"

namespace opossum {

class CalibrationQueryGenerator {
 public:
  explicit CalibrationQueryGenerator(const std::vector<std::pair<std::string, size_t>>& tables,
                                     const std::vector<CalibrationColumnSpecification>& column_specifications,
                                     const CalibrationConfiguration& configuration);

  const std::vector<std::shared_ptr<AbstractLQPNode>> generate_queries() const;

  static const std::vector<std::shared_ptr<AbstractLQPNode>> generate_tpch_6();
  static const std::vector<std::shared_ptr<AbstractLQPNode>> generate_tpch_12();

 private:
  const std::shared_ptr<AbstractLQPNode> _generate_query_for_predicate_chain_with_validate(
      const std::shared_ptr<StoredTableNode> table_node, const std::shared_ptr<AbstractExpression> data_table_predicate,
    const std::shared_ptr<AbstractExpression> reference_table_predicate, const ScanType scan_type) const;
  const std::shared_ptr<AbstractLQPNode> _generate_query_for_validate_and_predicate(
      const std::shared_ptr<StoredTableNode> table_node, const std::shared_ptr<AbstractExpression> reference_table_predicate,
      const ScanType scan_type) const;
  const std::vector<std::shared_ptr<AbstractLQPNode>> _generate_table_scan(
      const CalibrationQueryGeneratorPredicateConfiguration& configuration,
      const PredicateGeneratorFunctor& predicate_generator) const;
  const std::vector<std::shared_ptr<AbstractLQPNode>> _generate_aggregate(const std::string& table_name) const;
  const std::vector<std::shared_ptr<AbstractLQPNode>> _generate_join(
      const CalibrationQueryGeneratorJoinConfiguration& configuration) const;

  const std::vector<std::shared_ptr<AbstractLQPNode>> _generate_projection(
      const std::vector<std::shared_ptr<LQPColumnExpression>>& columns) const;

  const std::vector<CalibrationColumnSpecification> _column_specifications;
  const CalibrationConfiguration _configuration;
  const std::vector<std::pair<std::string, size_t>> _tables;
};

}  // namespace opossum
