#pragma once

#include <string>
#include <unordered_map>

#include "cost_model/cost.hpp"

namespace opossum {

/**
 * Linear Regression Model
 */
class LinearRegressionModel {
 public:
  explicit LinearRegressionModel(const std::unordered_map<std::string, float>& coefficients);

  Cost predict(const std::unordered_map<std::string, float>& features) const;

 private:
  const std::unordered_map<std::string, float> _coefficients;
};

}  // namespace opossum
