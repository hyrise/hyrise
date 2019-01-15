#pragma once

#include <string>

#include "cost_model/cost_model_adaptive.hpp"

namespace opossum {

class CostModelCoefficientReader {
 public:
  static const TableScanCoefficientsPerGroup read_table_scan_coefficients(const std::string& file_path = "");
  static const JoinCoefficientsPerGroup read_join_coefficients(const std::string& file_path = "");
};

}  // namespace opossum
