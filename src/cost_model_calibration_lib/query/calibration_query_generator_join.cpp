#include "calibration_query_generator_join.hpp"

#include <random>

#include "configuration/calibration_column_specification.hpp"
#include "expression/expression_functional.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

const std::vector<std::shared_ptr<AbstractLQPNode>> CalibrationQueryGeneratorJoin::generate_join(
    const JoinGeneratorFunctor& join_predicate_generator, const std::shared_ptr<StoredTableNode>& left_table,
    const std::shared_ptr<StoredTableNode>& right_table) {
  std::vector<JoinType> join_types = {JoinType::Hash, JoinType::NestedLoop, JoinType::MPSM, JoinType::SortMerge};
  std::vector<std::shared_ptr<AbstractLQPNode>> permutated_join_nodes{};

  const auto join_predicate = join_predicate_generator(left_table, right_table);

  for (const auto& join_type : join_types) {
    const auto join_node = JoinNode::make(JoinMode::Inner, join_predicate, join_type);
    permutated_join_nodes.push_back(join_node);
  }

  return permutated_join_nodes;
}

const std::shared_ptr<AbstractExpression> CalibrationQueryGeneratorJoin::generate_join_predicate(
    const std::shared_ptr<StoredTableNode>& left_table, const std::shared_ptr<StoredTableNode>& right_table) {
  static std::mt19937 engine((std::random_device()()));

  auto left_columns = left_table->get_columns();
  auto right_column = right_table->get_column("column_pk");

  std::shuffle(left_columns.begin(), left_columns.end(), engine);

  for (const auto& left_column : left_columns) {
    const auto left_column_expression = lqp_column_(left_column);
    const auto right_column_expression = lqp_column_(right_column);
    if (left_column_expression->data_type() != right_column_expression->data_type()) continue;

    return expression_functional::equals_(left_column_expression, right_column_expression);
  }

  return {};
}

}  // namespace opossum
