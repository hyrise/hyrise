#include "calibration_query_generator_join.hpp"

#include <random>

#include "configuration/calibration_column_specification.hpp"
#include "expression/expression_functional.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

    const std::vector<CalibrationQueryGeneratorJoinConfiguration>
    CalibrationQueryGeneratorJoin::generate_join_permutations(
            const std::vector<std::pair<std::string, size_t>>& tables,
            const CalibrationConfiguration& configuration) {
      std::vector<CalibrationQueryGeneratorJoinConfiguration> output{};

      // Generating all combinations
      for (const auto& data_type : configuration.data_types) {
        for (const auto& encoding : configuration.encodings) {
          for (const auto& left_table : tables) {
            for (const auto& right_table : tables) {
              if (left_table.second == right_table.second * 10) {
                output.push_back({left_table.first, right_table.first, encoding, data_type, false});
                output.push_back({left_table.first, right_table.first, encoding, data_type, true});
              }
            }
          }
        }
      }

      std::cout << "Generated " << output.size() << " Permutations for Joins" << std::endl;
      return output;
    }


const std::vector<std::shared_ptr<AbstractLQPNode>> CalibrationQueryGeneratorJoin::generate_join(
    const CalibrationQueryGeneratorJoinConfiguration& configuration,
    const JoinGeneratorFunctor& join_predicate_generator, const std::shared_ptr<StoredTableNode>& left_table,
    const std::shared_ptr<StoredTableNode>& right_table,
    const std::vector<CalibrationColumnSpecification>& column_definitions) {
//  std::vector<JoinType> join_types = {JoinType::Hash, JoinType::NestedLoop, JoinType::MPSM, JoinType::SortMerge};
  std::vector<JoinType> join_types = {JoinType::Hash, JoinType::SortMerge};
  std::vector<std::shared_ptr<AbstractLQPNode>> permutated_join_nodes{};

  const auto join_predicate = join_predicate_generator(configuration, left_table, right_table, column_definitions);

  if (!join_predicate) {
    std::cout << "Could not generate join predicate for configuration" << std::endl;
  }

  for (const auto& join_type : join_types) {
    const auto join_predicate_copy = join_predicate;
    const auto join_node = JoinNode::make(JoinMode::Inner, join_predicate_copy, join_type);
    permutated_join_nodes.push_back(join_node);
  }

  return permutated_join_nodes;
}

const std::shared_ptr<AbstractExpression> CalibrationQueryGeneratorJoin::generate_join_predicate(
    const CalibrationQueryGeneratorJoinConfiguration& configuration, const std::shared_ptr<StoredTableNode>& left_table,
    const std::shared_ptr<StoredTableNode>& right_table,
    const std::vector<CalibrationColumnSpecification>& column_definitions) {
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
    if (definition.type == configuration.data_type && definition.encoding == configuration.encoding_type &&
        definition.column_name != "column_pk") {
      return definition;
    }
  }

  return {};
}

}  // namespace opossum
