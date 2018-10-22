#pragma once

#include <json.hpp>

#include "operators/get_table.hpp"
#include "operators/join_hash.hpp"
#include "operators/projection.hpp"
#include "operators/table_scan.hpp"

#include "storage/base_segment.hpp"
#include "storage/encoding_type.hpp"

#include "feature/calibration_constant_hardware_features.hpp"
#include "feature/calibration_example.hpp"
#include "feature/calibration_features.hpp"
#include "feature/calibration_join_features.hpp"
#include "feature/calibration_projection_features.hpp"
#include "feature/calibration_runtime_hardware_features.hpp"
#include "feature/calibration_table_scan_features.hpp"

namespace opossum {

class CostModelFeatureExtractor {
 public:
  static const CalibrationExample extract_features(const std::shared_ptr<const AbstractOperator>& op);

 private:
  static const CalibrationFeatures _extract_general_features(const std::shared_ptr<const AbstractOperator>& op);
  static const CalibrationConstantHardwareFeatures _extract_constant_hardware_features();
  static const CalibrationRuntimeHardwareFeatures _extract_runtime_hardware_features();

  static const std::optional<CalibrationTableScanFeatures> _extract_features_for_operator(
      const std::shared_ptr<const TableScan>& op);
  static const std::optional<CalibrationProjectionFeatures> _extract_features_for_operator(
      const std::shared_ptr<const Projection>& op);
  static const std::optional<CalibrationJoinFeatures> _extract_features_for_operator(
      const std::shared_ptr<const JoinHash>& op);

  static std::pair<EncodingType, bool> _get_encoding_type_for_segment(const std::shared_ptr<BaseSegment>& segment);
  static size_t _get_memory_usage_for_column(const std::shared_ptr<const Table>& table, ColumnID column_id);
};

}  // namespace opossum
