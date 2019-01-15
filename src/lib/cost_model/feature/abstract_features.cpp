#include "abstract_features.hpp"

namespace opossum {
namespace cost_model {

const std::vector<std::string> AbstractFeatures::feature_names() const {
  const auto map = serialize();
  std::vector<std::string> feature_names;
  feature_names.reserve(map.size());
  for (const auto& elem : map) {
    feature_names.push_back(elem.first);
  }

  return feature_names;
}

}  // namespace cost_model
}  // namespace opossum