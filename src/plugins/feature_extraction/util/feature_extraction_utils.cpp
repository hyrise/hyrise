#include "feature_extraction_utils.hpp"

#include "logical_query_plan/stored_table_node.hpp"
#include "feature_extraction/feature_nodes/operator_feature_node.hpp"
#include "expression/expression_utils.hpp"

namespace opossum {

std::vector<std::shared_ptr<BaseTableFeatureNode>> find_base_tables(const std::shared_ptr<AbstractFeatureNode>& root_node) {
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

std::shared_ptr<BaseTableFeatureNode> match_base_table(const LQPColumnExpression& column_expression,
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


std::pair<std::shared_ptr<AbstractTableFeatureNode>, ColumnID> find_original_column(const std::shared_ptr<AbstractExpression>& expression, const std::shared_ptr<AbstractFeatureNode>& root_node) {
  Assert(root_node->type() == FeatureNodeType::Operator, "Traversal should start from Operator node");
  std::pair<std::shared_ptr<AbstractTableFeatureNode>, ColumnID> result;
  bool is_set = false;
  visit_feature_nodes(root_node, [&](const auto node) {
    if (node->type() == FeatureNodeType::Table) {
      Assert(expression->type == ExpressionType::LQPColumn, "only LQPColumnExpression can stem from Base Table");
      const auto column_id = static_cast<LQPColumnExpression&>(*expression).original_column_id;
      const auto& table_node = std::static_pointer_cast<AbstractTableFeatureNode>(node);
      result = std::make_pair(table_node, column_id);
      is_set = true;
      return FeatureNodeVisitation::DoNotVisitInputs;
    }

    const auto& operator_node = std::static_pointer_cast<OperatorFeatureNode>(node);

    const auto& output_table = operator_node->output_table();
    const auto target_column = find_expression_idx(*expression, operator_node->get_operator()->lqp_node->output_expressions());

    if (!target_column) {
      return FeatureNodeVisitation::DoNotVisitInputs;
    }

    if (output_table->column_is_reference(*target_column)) {
      return FeatureNodeVisitation::VisitInputs;
    }

    if (output_table->column_is_materialized(*target_column)) {
      result = std::make_pair(output_table, *target_column);
      is_set = true;
      return FeatureNodeVisitation::DoNotVisitInputs;
    }

    const auto& performance_data = output_table->performance_data();
    const auto& segment_encoding_matrix = performance_data.output_segment_types;

    if (std::any_of(segment_encoding_matrix.cbegin(), segment_encoding_matrix.cend(), [&](const auto& chunk_encodings){
      return chunk_encodings.at(*target_column).has_value();
    })) {
      output_table->set_column_materialized(*target_column);
      result = std::make_pair(output_table, *target_column);
      return FeatureNodeVisitation::DoNotVisitInputs;
    }

    output_table->set_column_reference(*target_column);
    return FeatureNodeVisitation::VisitInputs;
  });

  Assert(is_set, "No original column found");
  return result;
}

}  // namespace opossum
