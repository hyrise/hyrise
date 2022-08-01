#include "feature_extraction_utils.hpp"

#include "expression/expression_utils.hpp"
#include "feature_extraction/feature_nodes/operator_feature_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"

namespace opossum {

std::vector<std::shared_ptr<BaseTableFeatureNode>> find_base_tables(
    const std::shared_ptr<AbstractFeatureNode>& root_node) {
  Assert(root_node->type() == FeatureNodeType::Operator, "Traversal should start from Operator node");
  auto result = std::vector<std::shared_ptr<BaseTableFeatureNode>>{};
  visit_feature_nodes(root_node, [&](const auto node) {
    if (node->type() != FeatureNodeType::Table) {
      return FeatureNodeVisitation::VisitInputs;
    }
    const auto table_node = std::static_pointer_cast<AbstractTableFeatureNode>(node);
    Assert(table_node->is_base_table(), "Expected base table");
    result.emplace_back(std::static_pointer_cast<BaseTableFeatureNode>(table_node));
    return FeatureNodeVisitation::DoNotVisitInputs;
  });
  Assert(!result.empty(), "Expected base tables");
  return result;
}

std::shared_ptr<BaseTableFeatureNode> match_base_table(
    const LQPColumnExpression& column_expression,
    const std::vector<std::shared_ptr<BaseTableFeatureNode>>& base_tables) {
  Assert(!column_expression.original_node.expired(), "Corrupted Column Expression");
  const auto input_node = column_expression.original_node.lock();
  Assert(input_node->type == LQPNodeType::StoredTable, "Expected StoredTableNode");
  const auto& stored_table_node = static_cast<const StoredTableNode&>(*input_node);

  for (const auto& base_table : base_tables) {
    if (base_table->table_name() == stored_table_node.table_name) {
      return base_table;
    }
  }

  Fail("Did not find base table");
}

std::pair<std::shared_ptr<AbstractTableFeatureNode>, ColumnID> find_original_column(
    const std::shared_ptr<AbstractExpression>& expression, const std::shared_ptr<AbstractFeatureNode>& root_node) {
  Assert(root_node->type() == FeatureNodeType::Operator, "Traversal should start from Operator node");

  auto original_table = std::shared_ptr<AbstractTableFeatureNode>{};
  auto original_column = ColumnID{INVALID_COLUMN_ID};
  visit_feature_nodes(root_node, [&](const auto node) {
    if (node->type() == FeatureNodeType::Table) {
      Assert(expression->type == ExpressionType::LQPColumn, "only LQPColumnExpression can stem from Base Table");
      original_table = std::static_pointer_cast<AbstractTableFeatureNode>(node);
      original_column = static_cast<LQPColumnExpression&>(*expression).original_column_id;
      return FeatureNodeVisitation::DoNotVisitInputs;
    }

    const auto& operator_node = std::static_pointer_cast<OperatorFeatureNode>(node);
    const auto& output_table = operator_node->output_table();
    const auto target_column =
        find_expression_idx(*expression, operator_node->get_operator()->lqp_node->output_expressions());

    if (!target_column) {
      return FeatureNodeVisitation::DoNotVisitInputs;
    }

    if (output_table->column_is_materialized(*target_column) || output_table->table_type() == TableType::Data) {
      original_table = output_table;
      original_column = *target_column;
      return FeatureNodeVisitation::DoNotVisitInputs;
    }

    return FeatureNodeVisitation::VisitInputs;
  });

  Assert(original_table && original_column != INVALID_COLUMN_ID, "No original column found");
  return std::make_pair(original_table, original_column);
}

void feature_vector_to_stream(std::ostream& stream, const FeatureVector& feature_vector) {
  const auto element_count = feature_vector.size();
  for (auto element_id = size_t{0}; element_id < element_count; ++element_id) {
    stream << feature_vector[element_id];
    if (element_id + 1 < element_count) {
      stream << ";";
    }
  }
}

}  // namespace opossum
