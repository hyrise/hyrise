#include "feature/join_features.hpp"

namespace opossum {
namespace cost_model {

const std::map<std::string, AllTypeVariant> JoinFeatures::serialize() const {
  std::map<std::string, AllTypeVariant> join_features =
      {
          {"join_type", join_type_to_string.at(join_type)},
      }

  std::map<std::string, AllTypeVariant>
      serialized_left_join_column = left_join_column.serialize();
  std::map<std::string, AllTypeVariant> serialized_right_join_column = right_join_column.serialize();

  return join_features.merge(serialized_left_join_column).merge(serialized_right_join_column);
};

}  // namespace cost_model
}  // namespace opossum