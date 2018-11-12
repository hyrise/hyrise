#pragma once

#include "configuration/calibration_configuration.hpp"

namespace opossum {

class CostModelCalibrationTableGenerator {
 public:
  explicit CostModelCalibrationTableGenerator(const CalibrationConfiguration configuration, const size_t chunk_size);

  void load_calibration_tables() const;
  void load_tpch_tables(const float scale_factor) const;

 private:
  const size_t _chunk_size;
  const CalibrationConfiguration _configuration;
};

}  // namespace opossum
