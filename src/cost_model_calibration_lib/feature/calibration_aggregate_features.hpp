#pragma once

#include <vector>

#include "all_type_variant.hpp"

namespace opossum {

struct CalibrationAggregateFeatures {
    static const std::vector<std::string> feature_names;

  static const std::vector<AllTypeVariant> serialize(const std::optional<CalibrationAggregateFeatures>& features);
};

inline const std::vector<std::string> CalibrationAggregateFeatures::feature_names({});

inline const std::vector<AllTypeVariant> CalibrationAggregateFeatures::serialize(
    const std::optional<CalibrationAggregateFeatures>& features) {
  return {};
}

}  // namespace opossum
