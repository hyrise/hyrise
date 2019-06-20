#pragma once

#include "abstract_features.hpp"
#include "all_type_variant.hpp"

namespace opossum {
namespace cost_model {

struct RuntimeHardwareFeatures : AbstractFeatures {
  float current_memory_consumption_percentage = 0.0;
  size_t running_query_count = 0;
  size_t remaining_transaction_count = 0;

  const std::map<std::string, AllTypeVariant> serialize() const override;
  const std::unordered_map<std::string, float> to_cost_model_features() const override;
};

}  // namespace cost_model
}  // namespace opossum
