#pragma once

#include <string>
#include <vector>

#include "configuration/calibration_configuration.hpp"
#include "feature/calibration_features.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "operators/abstract_operator.hpp"

namespace opossum {

class CostModelCalibrationQueryRunner {
 public:
  explicit CostModelCalibrationQueryRunner(CalibrationConfiguration configuration);

  const std::vector<CalibrationFeatures> calibrate_query_from_lqp(const std::shared_ptr<AbstractLQPNode>& lqp) const;
  const std::vector<CalibrationFeatures> calibrate_query_from_sql(const std::string& sql) const;

 private:
  const std::vector<CalibrationFeatures> _evaluate_query_plan(
      const std::vector<std::shared_ptr<AbstractOperator>>& query_plans) const;
  void _traverse(const std::shared_ptr<const AbstractOperator>& op, std::vector<CalibrationFeatures>& features) const;

  const CalibrationConfiguration _configuration;
};

}  // namespace opossum
