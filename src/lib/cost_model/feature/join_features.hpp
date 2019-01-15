#pragma once

#include <string>
#include <vector>

#include "abstract_features.hpp"
#include "all_type_variant.hpp"
#include "column_features.hpp"
#include "storage/encoding_type.hpp"

namespace opossum {
namespace cost_model {

struct JoinFeatures : AbstractFeatures {
  //  JoinType join_type;
  ColumnFeatures left_join_column{"left"};
  ColumnFeatures right_join_column{"right"};

  const std::map<std::string, AllTypeVariant> serialize() const override;
  const std::unordered_map<std::string, float> to_cost_model_features() const override;
};

}  // namespace cost_model
}  // namespace opossum
