#pragma once

#include <json.hpp>

#include "operators/get_table.hpp"
#include "operators/join_hash.hpp"
#include "operators/projection.hpp"
#include "operators/table_scan.hpp"

#include "storage/encoding_type.hpp"
#include "storage/reference_segment.hpp"

namespace opossum {

class CostModelFeatureExtractor {
 public:
  static const nlohmann::json extract_features(const std::shared_ptr<const AbstractOperator>& op);

 private:
  static const nlohmann::json _extract_constant_hardware_features();
  static const nlohmann::json _extract_runtime_hardware_features();

  static const nlohmann::json _extract_features_for_operator(const std::shared_ptr<const TableScan>& op);
  static const nlohmann::json _extract_features_for_operator(const std::shared_ptr<const Projection>& op);
  static const nlohmann::json _extract_features_for_operator(const std::shared_ptr<const JoinHash>& op);

  static EncodingType _get_encoding_type_for_segment(const std::shared_ptr<ReferenceSegment>& reference_segment);
  static size_t _get_memory_usage_for_column(const std::shared_ptr<const Table>& table, ColumnID column_id);
};

}  // namespace opossum
