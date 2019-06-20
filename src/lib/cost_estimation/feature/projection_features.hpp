#pragma once

#include <string>
#include <vector>

#include "abstract_features.hpp"
#include "all_type_variant.hpp"
#include "storage/encoding_type.hpp"

namespace opossum {
namespace cost_model {

struct ProjectionFeatures : AbstractFeatures {
  size_t input_column_count = 0;
  size_t output_column_count = 0;

  const std::map<std::string, AllTypeVariant> serialize() const override;
  const std::unordered_map<std::string, float> to_cost_model_features() const override;
};

}  // namespace cost_model
}  // namespace opossum
