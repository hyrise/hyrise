#include "feature_types.hpp"

namespace opossum {

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

}  // namespace opossum
