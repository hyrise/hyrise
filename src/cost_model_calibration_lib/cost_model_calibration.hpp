#pragma once

#include <json.hpp>

#include <string>
#include <vector>

#include "configuration/calibration_configuration.hpp"
#include "feature/calibration_example.hpp"
#include "operators/abstract_operator.hpp"

namespace opossum {

class CostModelCalibration {
 public:
  explicit CostModelCalibration(CalibrationConfiguration configuration);

  void calibrate() const;
  void run_tpch() const;

 private:
  void _traverse(const std::shared_ptr<const AbstractOperator>& op, std::vector<CalibrationExample>& examples) const;
  void _write_result_csv(const std::string output_path, const nlohmann::json& configuration,
                         const std::vector<CalibrationExample>& examples) const;
  CalibrationConfiguration _configuration;
};

}  // namespace opossum
