#pragma once

#include <json.hpp>

#include <string>
#include <vector>

#include "cost_model_calibration/configuration/calibration_configuration.hpp"
#include "operators/abstract_operator.hpp"

namespace opossum {

class CostModelCalibration {
 public:
  explicit CostModelCalibration(CalibrationConfiguration configuration);

  void calibrate() const;
  void run_tpch() const;

 private:
  void _traverse(const std::shared_ptr<const AbstractOperator>& op,
                 std::map<std::string, nlohmann::json>& operators) const;
  void _write_result_json(const std::string output_path, const nlohmann::json& configuration,
                          const std::map<std::string, nlohmann::json>& operators) const;
  CalibrationConfiguration _configuration;
};

}  // namespace opossum
