#pragma once

#include <vector>

#include "all_type_variant.hpp"

namespace opossum {

struct CalibrationJoinFeatures {
  std::string join_type = "";

  static const std::vector<std::string> columns;

  static const std::vector<AllTypeVariant> serialize(const std::optional<CalibrationJoinFeatures>& features);
};

inline const std::vector<std::string> CalibrationJoinFeatures::columns({});

inline const std::vector<AllTypeVariant> CalibrationJoinFeatures::serialize(
    const std::optional<CalibrationJoinFeatures>& features) {
  return {};
}

}  // namespace opossum
