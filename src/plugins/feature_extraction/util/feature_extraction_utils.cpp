#include "feature_extraction_utils.hpp"

#include "logical_query_plan/stored_table_node.hpp"

namespace opossum {

std::vector<std::shared_ptr<TableFeatureNode>> find_base_tables(const std::shared_ptr<AbstractFeatureNode>& root_node) {
  Assert(root_node->type() == FeatureNodeType::Operator, "Traversal should start from Operator node");
  auto result = std::vector<std::shared_ptr<TableFeatureNode>>{};
  visit_feature_nodes(root_node, [&](const auto node) {
    if (node->type() != FeatureNodeType::Table) {
      return FeatureNodeVisitation::VisitInputs;
    }
    const auto table_node = std::static_pointer_cast<TableFeatureNode>(node);
    Assert(table_node->is_base_table(), "Expected base table");
    result.emplace_back(table_node);
    return FeatureNodeVisitation::DoNotVisitInputs;
  });
  Assert(!result.empty(), "Expected base tables");
  return result;
}

std::shared_ptr<TableFeatureNode> match_base_table(const LQPColumnExpression& column_expression,
                                                   const std::vector<std::shared_ptr<TableFeatureNode>>& base_tables) {
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

}  // namespace opossum
