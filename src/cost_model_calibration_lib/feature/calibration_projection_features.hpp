#pragma once

#include <vector>

#include "all_type_variant.hpp"

namespace opossum {

struct CalibrationProjectionFeatures {
  size_t input_column_count = 0;
  size_t output_column_count = 0;

    static const std::vector<std::string> feature_names;

  static const std::vector<AllTypeVariant> serialize(const std::optional<CalibrationProjectionFeatures>& features);
};

inline const std::vector<std::string> CalibrationProjectionFeatures::feature_names({"input_column_count",
                                                                              "output_column_count"});

inline const std::vector<AllTypeVariant> CalibrationProjectionFeatures::serialize(
    const std::optional<CalibrationProjectionFeatures>& features) {
  if (!features) {
    // Need to return values for all fields
    return {NullValue{}, NullValue{}};
  }

  return {static_cast<int32_t>(features->input_column_count), static_cast<int32_t>(features->output_column_count)};
}

}  // namespace opossum
