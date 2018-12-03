#pragma once

#include <string>
#include <vector>

#include "configuration/calibration_configuration.hpp"
#include "feature/calibration_example.hpp"

namespace opossum {

class CostModelCalibration {
 public:
  explicit CostModelCalibration(CalibrationConfiguration configuration);

  void run() const;

  void run_tpch6_costing() const;

 private:
  void _append_to_result_csv(const std::string& output_path, const std::vector<CalibrationExample>& examples) const;
  void _calibrate() const;
  const std::vector<std::string> _collect_csv_header_columns() const;
  void _run_tpch() const;
  void _write_csv_header(const std::string& output_path) const;

  const CalibrationConfiguration _configuration;
};

}  // namespace opossum
