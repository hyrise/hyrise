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

  void calibrate();

 private:
  void _traverse(const std::shared_ptr<const AbstractOperator>& op);

  std::map<std::string, nlohmann::json> _operators;
  CalibrationConfiguration _configuration;
};

}  // namespace opossum
