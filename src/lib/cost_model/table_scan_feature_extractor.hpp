#pragma once

#include "abstract_feature_extractor.hpp"

namespace opossum {

class TableScanFeatureExtractor : public AbstractFeatureExtractor {
 public:
  std::unordered_map<std::string, float> extract(const std::shared_ptr<AbstractLQPNode>& node) const override;
  std::unordered_map<std::string, float> extract(
      const std::shared_ptr<AbstractOperator>& abstract_operator) const override;
};

}  // namespace opossum
