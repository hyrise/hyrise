#include "linear_regression_model.hpp"

#include "utils/assert.hpp"

namespace opossum {

LinearRegressionModel::LinearRegressionModel(const std::unordered_map<std::string, float>& coefficients)
    : _coefficients(coefficients) {}

Cost LinearRegressionModel::predict(const std::unordered_map<std::string, float>& features) const {
  Cost cost = 0;

  for (const auto& [coefficient, coefficient_value] : _coefficients) {
    Assert(features.count(coefficient), "Missing feature in LinearRegressionModel: " + coefficient);

    const auto feature_value = features.at(coefficient);
    cost += feature_value * coefficient_value;
  }

  return cost;
}

}  // namespace opossum
