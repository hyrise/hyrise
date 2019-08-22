#pragma once

#include "configuration/calibration_configuration.hpp"
#include "types.hpp"

namespace opossum {

class CostModelCalibrationTableGenerator {
 public:
  explicit CostModelCalibrationTableGenerator(const CalibrationConfiguration configuration,
                                              const ChunkOffset chunk_size);

  void load_calibration_tables() const;
  void generate_calibration_tables() const;
  void load_tpch_tables(const float scale_factor, const EncodingType encoding = EncodingType::Dictionary) const;
  void load_tpcds_tables(const float scale_factor, const EncodingType encoding = EncodingType::Dictionary) const;

 private:
  const ChunkOffset _chunk_size;
  const CalibrationConfiguration _configuration;
};

}  // namespace opossum
