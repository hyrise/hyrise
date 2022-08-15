#include "feature_types.hpp"

namespace hyrise {

QueryOperatorType map_operator_type(const OperatorType operator_type) {
  Assert(operator_type_mapping.contains(operator_type),
         "Did not expect '" + std::string{magic_enum::enum_name(operator_type)} + "' operator in a query");
  return operator_type_mapping.at(operator_type);
}

void feature_vector_to_stream(const FeatureVector& feature_vector, std::ostream& stream) {
  for (auto it = feature_vector.cbegin(); it < feature_vector.cend(); ++it) {
    stream << *it;
    if (std::next(it) != feature_vector.cend()) {
      stream << ",";
    }
  }
}

std::string feature_vector_to_string(const FeatureVector& feature_vector) {
  std::stringstream ss;
  feature_vector_to_stream(feature_vector, ss);
  return ss.str();
}

std::ostream& operator<<(std::ostream& stream, const FeatureVector& feature_vector) {
  feature_vector_to_stream(feature_vector, stream);
  return stream;
}

}  // namespace hyrise
