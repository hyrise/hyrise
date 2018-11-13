#pragma once

#include <vector>

#include "all_type_variant.hpp"
#include "calibration_aggregate_features.hpp"
#include "calibration_constant_hardware_features.hpp"
#include "calibration_features.hpp"
#include "calibration_join_features.hpp"
#include "calibration_projection_features.hpp"
#include "calibration_runtime_hardware_features.hpp"
#include "calibration_table_scan_features.hpp"

namespace opossum {

struct CalibrationExample {
  CalibrationFeatures calibration_features;
  CalibrationConstantHardwareFeatures hardware_features;
  CalibrationRuntimeHardwareFeatures runtime_features;

  std::optional<CalibrationAggregateFeatures> aggregate_features = std::nullopt;
  std::optional<CalibrationJoinFeatures> join_features = std::nullopt;
  std::optional<CalibrationProjectionFeatures> projection_features = std::nullopt;
  std::optional<CalibrationTableScanFeatures> table_scan_features = std::nullopt;
};

inline std::vector<AllTypeVariant> serialize(const CalibrationExample& example) {
  std::vector<AllTypeVariant> all_type_variants{};

  const auto calibration_features = CalibrationFeatures::serialize(example.calibration_features);
  const auto hardware_features = CalibrationConstantHardwareFeatures::serialize(example.hardware_features);
  const auto runtime_features = CalibrationRuntimeHardwareFeatures::serialize(example.runtime_features);
  const auto aggregate_features = CalibrationAggregateFeatures::serialize(example.aggregate_features);
  const auto join_features = CalibrationJoinFeatures::serialize(example.join_features);
  const auto projection_features = CalibrationProjectionFeatures::serialize(example.projection_features);
  const auto table_scan_features = CalibrationTableScanFeatures::serialize(example.table_scan_features);

  all_type_variants.insert(std::end(all_type_variants), std::begin(calibration_features),
                           std::end(calibration_features));
  all_type_variants.insert(std::end(all_type_variants), std::begin(hardware_features), std::end(hardware_features));
  all_type_variants.insert(std::end(all_type_variants), std::begin(runtime_features), std::end(runtime_features));
  all_type_variants.insert(std::end(all_type_variants), std::begin(aggregate_features), std::end(aggregate_features));
  all_type_variants.insert(std::end(all_type_variants), std::begin(join_features), std::end(join_features));
  all_type_variants.insert(std::end(all_type_variants), std::begin(projection_features), std::end(projection_features));
  all_type_variants.insert(std::end(all_type_variants), std::begin(table_scan_features), std::end(table_scan_features));

  return all_type_variants;
}

}  // namespace opossum
