#pragma once

#include <string>
#include <vector>

#include "all_type_variant.hpp"
#include "feature/calibration_column_features.hpp"
#include "storage/encoding_type.hpp"

namespace opossum {

struct CalibrationTableScanFeatures {
  CalibrationColumnFeatures first_column{};
  CalibrationColumnFeatures second_column{};
  CalibrationColumnFeatures third_column{};

  bool is_column_comparison = false;
  std::string scan_operator_type = "undefined";
  size_t number_of_computable_or_column_expressions = 0;
  size_t number_of_effective_chunks = 0;

  static const std::vector<std::string> feature_names;

  static const std::vector<AllTypeVariant> serialize(const std::optional<CalibrationTableScanFeatures>& features);
};

inline const std::vector<std::string> CalibrationTableScanFeatures::feature_names = {
    []() {
      const auto first_column_names = CalibrationColumnFeatures::feature_names_with_prefix("first_column");
      const auto second_column_names = CalibrationColumnFeatures::feature_names_with_prefix("second_column");
      const auto third_column_names = CalibrationColumnFeatures::feature_names_with_prefix("third_column");

      std::vector<std::string> output{};

      output.insert(output.end(), first_column_names.begin(), first_column_names.end());
      output.insert(output.end(), second_column_names.begin(), second_column_names.end());
      output.insert(output.end(), third_column_names.begin(), third_column_names.end());
      output.emplace_back("is_column_comparison");
      output.emplace_back("scan_operator_type");
      output.emplace_back("number_of_computable_or_column_expressions");
      output.emplace_back("number_of_effective_chunks");

      return output;
    }()
};

inline const std::vector<AllTypeVariant> CalibrationTableScanFeatures::serialize(
    const std::optional<CalibrationTableScanFeatures>& features) {
  if (!features) {
    const auto serialized_column = CalibrationColumnFeatures::serialize({});
    const auto column_feature_count = 3 * serialized_column.size();

    // +4 for the three table scan features
    std::vector<AllTypeVariant> output(column_feature_count + 4, NullValue{});
    return output;
  }

  std::vector<AllTypeVariant> output{};

  const auto serialized_first_column = CalibrationColumnFeatures::serialize(features->first_column);
  const auto serialized_second_column = CalibrationColumnFeatures::serialize(features->second_column);
  const auto serialized_third_column = CalibrationColumnFeatures::serialize(features->third_column);

  output.insert(output.end(), serialized_first_column.begin(), serialized_first_column.end());
  output.insert(output.end(), serialized_second_column.begin(), serialized_second_column.end());
  output.insert(output.end(), serialized_third_column.begin(), serialized_third_column.end());
  output.emplace_back(features->is_column_comparison);
  output.emplace_back(features->scan_operator_type);
  output.emplace_back(static_cast<int32_t>(features->number_of_computable_or_column_expressions));
  output.emplace_back(static_cast<int32_t>(features->number_of_effective_chunks));

  return output;
}

}  // namespace opossum
