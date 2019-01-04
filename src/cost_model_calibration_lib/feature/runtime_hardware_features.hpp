//#pragma once
//
//#include <vector>
//
//#include "all_type_variant.hpp"
//
//namespace opossum {
//
//struct CalibrationRuntimeHardwareFeatures {
//  size_t current_memory_consumption_percentage = 0;
//  size_t running_queries = 0;
//  size_t remaining_transactions = 0;
//
//  static const std::vector<std::string> feature_names;
//
//  static const std::vector<AllTypeVariant> serialize(const CalibrationRuntimeHardwareFeatures& features);
//};
//
//inline const std::vector<std::string> CalibrationRuntimeHardwareFeatures::feature_names(
//    {"current_memory_consumption_percentage", "running_queries", "remaining_transactions"});
//
//inline const std::vector<AllTypeVariant> CalibrationRuntimeHardwareFeatures::serialize(
//    const CalibrationRuntimeHardwareFeatures& features) {
//  return {static_cast<int32_t>(features.current_memory_consumption_percentage),
//          static_cast<int32_t>(features.running_queries), static_cast<int32_t>(features.remaining_transactions)};
//}
//
//}  // namespace opossum

#pragma once

#include "all_type_variant.hpp"
#include "feature/abstract_features.hpp"

namespace opossum {
namespace cost_model {

struct RuntimeHardwareFeatures : AbstractFeatures {
  size_t current_memory_consumption_percentage = 0;
  size_t running_query_count = 0;
  size_t remaining_transaction_count = 0;

    const std::map<std::string, AllTypeVariant> serialize() const override;
};

}  // namespace cost_model
}  // namespace opossum
