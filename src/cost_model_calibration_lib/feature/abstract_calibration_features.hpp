#pragma once

#include <vector>

#include "all_type_variant.hpp"

namespace opossum {

struct AbstractCalibrationFeatures {
  static const std::vector<std::string> columns;

  static const std::vector<std::string> feature_names(const std::optional<std::string>& prefix = {});
  static const std::vector<AllTypeVariant> serialize(const std::optional<CalibrationAggregateFeatures>& features);
};

inline const std::vector<std::string> AbstractCalibrationFeatures::feature_names(
    const std::optional<std::string>& prefix) {
  if (!prefix) {
    return columns;
  }

  std::for_each(columns.begin(), columns.end(), [prefix](auto& s) { s.insert(0, prefix); });
  return columns;
}

}  // namespace opossum
