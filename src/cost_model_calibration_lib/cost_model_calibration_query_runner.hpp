#pragma once

#include <string>
#include <vector>

#include "configuration/calibration_configuration.hpp"
#include "feature/calibration_example.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "operators/abstract_operator.hpp"
#include "sql/sql_query_plan.hpp"

namespace opossum {

class CostModelCalibrationQueryRunner {
 public:
  explicit CostModelCalibrationQueryRunner(CalibrationConfiguration configuration);

  const std::vector<CalibrationExample> calibrate_query_from_lqp(const std::shared_ptr<AbstractLQPNode>& lqp) const;
  const std::vector<CalibrationExample> calibrate_query_from_sql(const std::string& sql) const;

 private:
  const std::vector<CalibrationExample> _evaluate_query_plan(
      const std::vector<std::shared_ptr<SQLQueryPlan>>& query_plans) const;
  void _traverse(const std::shared_ptr<const AbstractOperator>& op, std::vector<CalibrationExample>& examples) const;

  const CalibrationConfiguration _configuration;
};

}  // namespace opossum
