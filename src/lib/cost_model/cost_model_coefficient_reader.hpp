#pragma once

#include <string>

#include "cost_model/cost_model_adaptive.hpp"

namespace opossum {

class CostModelCoefficientReader {
 public:
  static const CoefficientsPerGroup default_coefficients();
  static const CoefficientsPerGroup read_table_scan_coefficients(const std::string& file_path = "");
  static const CoefficientsPerGroup read_join_coefficients(const std::string& file_path = "");
};

}  // namespace opossum
