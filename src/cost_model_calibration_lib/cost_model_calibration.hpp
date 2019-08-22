#pragma once

#include <mutex>
#include <string>
#include <vector>

#include "configuration/calibration_configuration.hpp"
#include "cost_estimation/feature/cost_model_features.hpp"

namespace opossum {

class CostModelCalibration {
 public:
  explicit CostModelCalibration(CalibrationConfiguration configuration);

  void run();

  void run_tpch6_costing();

 private:
  void _append_to_result_csv(const std::string& output_path,
                             const std::vector<cost_model::CostModelFeatures>& features);
  void _calibrate();
  void _run_tpch();
  void _run_tpcc();
  void _write_csv_header(const std::string& output_path);

  const CalibrationConfiguration _configuration;
  std::mutex _csv_write_mutex;
};

}  // namespace opossum
