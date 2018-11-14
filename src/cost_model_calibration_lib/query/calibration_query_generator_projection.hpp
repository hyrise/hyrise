#pragma once

#include "logical_query_plan/lqp_column_reference.hpp"
#include "logical_query_plan/projection_node.hpp"

namespace opossum {

class CalibrationQueryGeneratorProjection {
public:
  static const std::shared_ptr<ProjectionNode> generate_projection(const std::vector<LQPColumnReference>& columns);

 private:
    CalibrationQueryGeneratorProjection() = default;
};

}  // namespace opossum
