#pragma once

#include <vector>

#include "all_type_variant.hpp"

namespace opossum {

struct CalibrationJoinFeatures {
  static const std::vector<std::string> columns;
};

inline const std::vector<std::string> CalibrationJoinFeatures::columns({});

inline std::vector<AllTypeVariant> serialize(const std::optional<CalibrationJoinFeatures>& features) { return {}; }

}  // namespace opossum
