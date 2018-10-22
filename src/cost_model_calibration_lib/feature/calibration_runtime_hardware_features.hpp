#pragma once

#include <vector>

#include "all_type_variant.hpp"

namespace opossum {

struct CalibrationRuntimeHardwareFeatures {
  size_t current_memory_consumption_percentage = 0;
  size_t running_queries = 0;
  size_t remaining_transactions = 0;

  static const std::vector<std::string> columns;
};

inline const std::vector<std::string> CalibrationRuntimeHardwareFeatures::columns(
    {"current_memory_consumption_percentage", "running_queries", "remaining_transactions"});

inline std::vector<AllTypeVariant> serialize(const CalibrationRuntimeHardwareFeatures& features) {
  return {static_cast<int32_t>(features.current_memory_consumption_percentage),
          static_cast<int32_t>(features.running_queries), static_cast<int32_t>(features.remaining_transactions)};
}

}  // namespace opossum
