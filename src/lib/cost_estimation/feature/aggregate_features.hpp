#pragma once

#include <map>

#include "abstract_features.hpp"

namespace opossum {
namespace cost_model {

struct AggregateFeatures : public AbstractFeatures {
  size_t aggregate_column_count = 0;
  size_t group_by_column_count = 0;

  const std::map<std::string, AllTypeVariant> serialize() const override;
  const std::unordered_map<std::string, float> to_cost_model_features() const override;
};

}  // namespace cost_model
}  // namespace opossum
