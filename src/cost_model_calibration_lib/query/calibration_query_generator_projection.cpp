#include "calibration_query_generator_projection.hpp"

#include <random>

#include "expression/expression_functional.hpp"
#include "logical_query_plan/projection_node.hpp"

namespace opossum {

const std::shared_ptr<ProjectionNode> CalibrationQueryGeneratorProjection::generate_projection(
    const std::vector<std::shared_ptr<LQPColumnExpression>>& columns) {
  static std::mt19937 engine((std::random_device()()));

  std::uniform_int_distribution<u_int64_t> dist(1, columns.size());

  std::vector<std::shared_ptr<LQPColumnExpression>> sampled;
  std::sample(columns.begin(), columns.end(), std::back_inserter(sampled), dist(engine), engine);

  std::vector<std::shared_ptr<AbstractExpression>> column_expressions{};
  column_expressions.reserve(sampled.size());
  for (const auto& column_ref : sampled) {
    column_expressions.push_back(column_ref);
  }

  return ProjectionNode::make(column_expressions);
}

}  // namespace opossum
