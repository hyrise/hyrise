#pragma once

#include "logical_query_plan/projection_node.hpp"

namespace opossum {

class CalibrationQueryGeneratorProjection {
 public:
  static const std::shared_ptr<ProjectionNode> generate_projection(const std::vector<std::shared_ptr<LQPColumnExpression>>& columns);

 private:
  CalibrationQueryGeneratorProjection() = default;
};

}  // namespace opossum
