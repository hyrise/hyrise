#include "feature/join_features.hpp"

#include "constant_mappings.hpp"

namespace opossum {
namespace cost_model {

const std::map<std::string, AllTypeVariant> JoinFeatures::serialize() const {
  std::map<std::string, AllTypeVariant> join_features =
      {
          {"join_type", join_type_to_string.at(join_type)},
      };

  std::map<std::string, AllTypeVariant> serialized_left_join_column = left_join_column.serialize();
  std::map<std::string, AllTypeVariant> serialized_right_join_column = right_join_column.serialize();

  join_features.insert(serialized_left_join_column.begin(), serialized_left_join_column.end());
  join_features.insert(serialized_right_join_column.begin(), serialized_right_join_column.end());

  return join_features;
};

}  // namespace cost_model
}  // namespace opossum