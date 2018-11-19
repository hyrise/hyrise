#include "calibration_query_generator_join.hpp"

#include <random>

#include "configuration/calibration_column_specification.hpp"
#include "expression/expression_functional.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

const std::vector<std::shared_ptr<AbstractLQPNode>> CalibrationQueryGeneratorJoin::generate_join(
    const CalibrationQueryGeneratorJoinConfiguration& configuration,
    const JoinGeneratorFunctor& join_predicate_generator,
    const std::shared_ptr<StoredTableNode>& left_table,
    const std::shared_ptr<StoredTableNode>& right_table, const std::vector<CalibrationColumnSpecification>& column_definitions) {
  std::vector<JoinType> join_types = {JoinType::Hash, JoinType::NestedLoop, JoinType::MPSM, JoinType::SortMerge};
  std::vector<std::shared_ptr<AbstractLQPNode>> permutated_join_nodes{};

  const auto join_predicate = join_predicate_generator(configuration, left_table, right_table, column_definitions);

  for (const auto& join_type : join_types) {
    const auto join_node = JoinNode::make(JoinMode::Inner, join_predicate, join_type);
    permutated_join_nodes.push_back(join_node);
  }

  return permutated_join_nodes;
}

const std::shared_ptr<AbstractExpression> CalibrationQueryGeneratorJoin::generate_join_predicate(
    const CalibrationQueryGeneratorJoinConfiguration& configuration, const std::shared_ptr<StoredTableNode>& left_table,
    const std::shared_ptr<StoredTableNode>& right_table, const std::vector<CalibrationColumnSpecification>& column_definitions) {
  const auto left_column_definition = _find_column_for_configuration(column_definitions, configuration);
  if (!left_column_definition) return {};

  const auto left_column = left_table->get_column(left_column_definition->column_name);
  const auto left_column_expression = lqp_column_(left_column);

  const auto right_column = right_table->get_column("column_pk");
  const auto right_column_expression = lqp_column_(right_column);

  return expression_functional::equals_(left_column_expression, right_column_expression);
}

    const std::optional<CalibrationColumnSpecification> CalibrationQueryGeneratorJoin::_find_column_for_configuration(
            const std::vector<CalibrationColumnSpecification>& column_definitions,
            const CalibrationQueryGeneratorJoinConfiguration& configuration) {
      for (const auto& definition : column_definitions) {
        if (definition.type == configuration.data_type
        && definition.encoding == configuration.encoding_type
        && definition.column_name != "column_pk") {
          return definition;
        }
      }

      return {};
    }

}  // namespace opossum
