#pragma once

#include <memory>

#include "cost_estimation/feature/cost_model_features.hpp"

namespace opossum {
namespace cost_model {

/**
 * Interface of an algorithm that predicts Cost for operators.
 */
class AbstractFeatureExtractor {
 public:
  virtual ~AbstractFeatureExtractor() = default;

  virtual const CostModelFeatures extract_features(const std::shared_ptr<AbstractLQPNode>& node) const = 0;
};

}  // namespace cost_model
}  // namespace opossum
