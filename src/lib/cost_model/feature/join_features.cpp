#include "join_features.hpp"

#include "constant_mappings.hpp"

namespace opossum {
namespace cost_model {

const std::map<std::string, AllTypeVariant> JoinFeatures::serialize() const {
  std::map<std::string, AllTypeVariant> join_features = {//      {"join_type", join_type_to_string.at(join_type)},
                                                         {"join_mode", join_mode_to_string.at(join_mode)}};

  const auto serialized_left_join_column = left_join_column.serialize();
  const auto serialized_right_join_column = right_join_column.serialize();

  join_features.insert(serialized_left_join_column.begin(), serialized_left_join_column.end());
  join_features.insert(serialized_right_join_column.begin(), serialized_right_join_column.end());

  return join_features;
}

const std::unordered_map<std::string, float> JoinFeatures::to_cost_model_features() const {
  std::unordered_map<std::string, float> join_features = {
      //      {"join_type", join_type_to_string.at(join_type)},
  };

  const auto serialized_left_join_column = left_join_column.to_cost_model_features();
  const auto serialized_right_join_column = right_join_column.to_cost_model_features();

  join_features.insert(serialized_left_join_column.begin(), serialized_left_join_column.end());
  join_features.insert(serialized_right_join_column.begin(), serialized_right_join_column.end());

  return join_features;
}

}  // namespace cost_model
}  // namespace opossum