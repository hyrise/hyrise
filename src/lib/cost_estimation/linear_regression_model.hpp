#pragma once

#include <string>
#include <unordered_map>

#include "cost_estimation/cost.hpp"

namespace opossum {

using ModelCoefficients = std::unordered_map<std::string, float>;
/**
 * Linear Regression Model
 */
class LinearRegressionModel {
 public:
  explicit LinearRegressionModel(const ModelCoefficients& coefficients);

  Cost predict(const std::unordered_map<std::string, float>& features) const;

 private:
  const std::unordered_map<std::string, float> _coefficients;
};

}  // namespace opossum
