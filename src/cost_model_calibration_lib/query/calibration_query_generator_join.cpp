#include "calibration_query_generator_join.hpp"

#include <boost/algorithm/string.hpp>
#include <random>

#include "configuration/calibration_column_specification.hpp"
#include "expression/expression_functional.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

CalibrationQueryGeneratorJoin::CalibrationQueryGeneratorJoin(
    const CalibrationQueryGeneratorJoinConfiguration& configuration,
    const std::vector<CalibrationColumnSpecification>& column_definitions)
    : _configuration(configuration), _column_definitions(column_definitions) {}

const std::vector<CalibrationQueryGeneratorJoinConfiguration> CalibrationQueryGeneratorJoin::generate_join_permutations(
    const std::vector<std::pair<std::string, size_t>>& tables, const CalibrationConfiguration& configuration) {
  std::vector<CalibrationQueryGeneratorJoinConfiguration> output{};

  // Generating all combinations
  for (const auto& data_type : configuration.data_types) {
    for (const auto& left_encoding : configuration.encodings) {
      for (const auto& right_encoding : configuration.encodings) {
        for (const auto& left_table : tables) {
          for (const auto& right_table : tables) {
            const auto table_ratio = static_cast<double>(right_table.second) / static_cast<double>(left_table.second);
            for (const auto ratio : {0.001, 0.01, 0.1, 1.0, 10.0, 100.0, 1000.0}) {
              if (table_ratio == ratio) {
                // With and without ReferenceSegments
                output.push_back({left_table.first, right_table.first, left_table.second, right_table.second,
                                  left_encoding, right_encoding, data_type, false, ratio});
                output.push_back({left_table.first, right_table.first, left_table.second, right_table.second,
                                  left_encoding, right_encoding, data_type, true, ratio});
              }
            }
          }
        }
      }
    }
  }

  std::cout << "Generated " << output.size() << " Permutations for Joins" << std::endl;
  return output;
}

const std::vector<std::shared_ptr<AbstractLQPNode>> CalibrationQueryGeneratorJoin::generate_join(
    const std::shared_ptr<StoredTableNode>& left_table, const std::shared_ptr<StoredTableNode>& right_table) const {
  std::vector<JoinType> join_types = {JoinType::Hash, JoinType::Index, JoinType::NestedLoop, JoinType::SortMerge};

  const auto join_predicate = _generate_join_predicate(left_table, right_table);

  if (!join_predicate) {
    std::cout << "Could not generate join predicate for configuration " << _configuration << std::endl;
    return {};
  }

  std::vector<std::shared_ptr<AbstractLQPNode>> permutated_join_nodes{};
  for (const auto& join_type : join_types) {
    // There is a bug in JoinIndex for large tables.
    // Avoid running JoinNestedLoop for too large tables.
    if ((_configuration.left_table_size > 100000 || _configuration.right_table_size > 100000) &&
        (join_type == JoinType::NestedLoop || join_type == JoinType::Index)) {
      continue;
    }
    // Running LeftOuterJoin in order to prevent HashJoin from swapping inputs
    const auto join_node = JoinNode::make(JoinMode::Left, join_predicate, join_type);
    permutated_join_nodes.push_back(join_node);
  }

  return permutated_join_nodes;
}

const std::shared_ptr<AbstractExpression> CalibrationQueryGeneratorJoin::_generate_join_predicate(
    const std::shared_ptr<StoredTableNode>& left_table, const std::shared_ptr<StoredTableNode>& right_table) const {
  // TODO(Sven): Refactor
  if (_configuration.table_ratio < 1) {
    const auto ratio = 1 / _configuration.table_ratio;
    const auto left_column_definition = _find_primary_key();
    if (!left_column_definition) return {};

    const auto left_column_expression = left_table->get_column(left_column_definition->column_name);

    const auto right_column_definition = _find_foreign_key(ratio);
    if (!right_column_definition) return {};
    const auto right_column_expression = right_table->get_column(right_column_definition->column_name);

    return expression_functional::equals_(left_column_expression, right_column_expression);
  } else {
    const auto left_column_definition = _find_foreign_key(_configuration.table_ratio);
    if (!left_column_definition) return {};

    const auto left_column_expression = left_table->get_column(left_column_definition->column_name);

    const auto right_column_definition = _find_primary_key();
    if (!right_column_definition) return {};
    const auto right_column_expression = right_table->get_column(right_column_definition->column_name);

    return expression_functional::equals_(left_column_expression, right_column_expression);
  }
}

const std::optional<CalibrationColumnSpecification> CalibrationQueryGeneratorJoin::_find_primary_key() const {
  for (const auto& definition : _column_definitions) {
    if (definition.data_type == _configuration.data_type &&
        definition.encoding.encoding_type == _configuration.left_encoding_type &&
        boost::algorithm::starts_with(definition.column_name, "column_pk")) {
      return definition;
    }
  }

  return {};
}

const std::optional<CalibrationColumnSpecification> CalibrationQueryGeneratorJoin::_find_foreign_key(
    const double table_ratio) const {
  for (const auto& definition : _column_definitions) {
    // In case of _configuration.table_ratio == 1, this will select the primary key
    if (definition.data_type == _configuration.data_type &&
        definition.encoding.encoding_type == _configuration.right_encoding_type && static_cast<double>(definition.fraction) == table_ratio) {
      return definition;
    }
  }

  return {};
}

}  // namespace opossum
