#pragma once

#include <json.hpp>

#include "operators/get_table.hpp"
#include "operators/join_hash.hpp"
#include "operators/projection.hpp"
#include "operators/table_scan.hpp"

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

  static const nlohmann::json _extract_features_for_segment(const std::shared_ptr<BaseSegment>& segment, const std::string& prefix);
};

}  // namespace opossum
