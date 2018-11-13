#pragma once

#include <vector>

#include "all_type_variant.hpp"
#include "feature/calibration_column_features.hpp"

namespace opossum {

struct CalibrationJoinFeatures {
  std::string join_type = "";
  CalibrationColumnFeatures left_join_column {};
  CalibrationColumnFeatures right_join_column {};

  static const std::vector<std::string> feature_names;

  static const std::vector<AllTypeVariant> serialize(const std::optional<CalibrationJoinFeatures>& features);
};

inline const std::vector<std::string> CalibrationJoinFeatures::feature_names(
        [](){
            const auto left_join_column_names = CalibrationColumnFeatures::feature_names_with_prefix("left_join_column");
            const auto right_join_column_names = CalibrationColumnFeatures::feature_names_with_prefix("right_join_column");

            std::vector<std::string> output {};

            output.insert(output.end(), left_join_column_names.begin(), left_join_column_names.end());
            output.insert(output.end(), right_join_column_names.begin(), right_join_column_names.end());
            output.emplace_back("join_type");

            return output;
        }()
        );

inline const std::vector<AllTypeVariant> CalibrationJoinFeatures::serialize(
    const std::optional<CalibrationJoinFeatures>& features) {
  if (!features) {
    const auto serialized_column = CalibrationColumnFeatures::serialize({});
    const auto column_feature_count = 2 * serialized_column.size();

    // +1 for the join feature
    std::vector<AllTypeVariant> output (column_feature_count + 1, NullValue{});
    return output;
  }

  std::vector<AllTypeVariant> output{};

  const auto serialized_left_join_column = CalibrationColumnFeatures::serialize(features->left_join_column);
  const auto serialized_right_join_column = CalibrationColumnFeatures::serialize(features->right_join_column);

  output.insert(output.end(), serialized_left_join_column.begin(), serialized_left_join_column.end());
  output.insert(output.end(), serialized_right_join_column.begin(), serialized_right_join_column.end());
  output.emplace_back(features->join_type);

  return output;
}

}  // namespace opossum
