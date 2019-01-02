#pragma once

#include <string>
#include <unordered_map>

#include "logical_query_plan/abstract_lqp_node.hpp"
#include "operators/abstract_operator.hpp"

namespace opossum {

class AbstractFeatureExtractor {
 public:
  virtual ~AbstractFeatureExtractor() = default;

  virtual std::unordered_map<std::string, float> extract(const std::shared_ptr<AbstractLQPNode>& lqp) const = 0;
  virtual std::unordered_map<std::string, float> extract(
      const std::shared_ptr<AbstractOperator>& abstract_operator) const = 0;
};

}  // namespace opossum
