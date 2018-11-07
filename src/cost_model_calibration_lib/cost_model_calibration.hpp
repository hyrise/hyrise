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

  void load_tables() const;
  void load_tpch_tables() const;

  void calibrate() const;
  void run_tpch() const;

 private:
  const std::vector<CalibrationExample> _calibrate_query(const std::string& query) const;
  void _traverse(const std::shared_ptr<const AbstractOperator>& op, std::vector<CalibrationExample>& examples) const;
  void _write_csv_header(const std::string& output_path) const;
  void _append_to_result_csv(const std::string& output_path, const std::vector<CalibrationExample>& examples) const;

  const CalibrationConfiguration _configuration;
};

}  // namespace opossum
