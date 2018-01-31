#include "build_lqps.hpp"

#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/create_view_node.hpp"
#include "logical_query_plan/delete_node.hpp"
#include "logical_query_plan/drop_view_node.hpp"
#include "logical_query_plan/dummy_table_node.hpp"
#include "logical_query_plan/insert_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/limit_node.hpp"
#include "logical_query_plan/logical_plan_root_node.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/show_columns_node.hpp"
#include "logical_query_plan/show_tables_node.hpp"
#include "logical_query_plan/sort_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "logical_query_plan/update_node.hpp"
#include "logical_query_plan/validate_node.hpp"

namespace opossum {

std::shared_ptr<PredicateNode> make_predicate_node(const LQPColumnReference& column_reference, const PredicateCondition predicate_condition, const AllParameterVariant& value, const std::shared_ptr<AbstractLQPNode>& child) {
  return make_predicate_node(column_reference, predicate_condition, value, std::nullopt, child);
}

std::shared_ptr<PredicateNode> make_predicate_node(const LQPColumnReference& column_reference, const PredicateCondition predicate_condition, const AllParameterVariant& value, const std::optional<AllTypeVariant>& value2, const std::shared_ptr<AbstractLQPNode>& child) {
  const auto predicate_node =  std::make_shared<PredicateNode>(column_reference, predicate_condition, value, value2);
  predicate_node->set_left_child(child);
  return predicate_node;
}

std::shared_ptr<ProjectionNode> make_star_projection_node(const std::shared_ptr<AbstractLQPNode>& child) {
  std::vector<std::shared_ptr<LQPExpression>> expressions = LQPExpression::create_columns(child->output_column_references());
  return make_projection_node(expressions, child);
}

std::shared_ptr<ProjectionNode> make_projection_node(const std::vector<std::shared_ptr<LQPExpression>>& column_expressions, const std::shared_ptr<AbstractLQPNode>& child) {
  const auto projection_node = std::make_shared<ProjectionNode>(column_expressions);
  projection_node->set_left_child(child);
  return projection_node;
}

std::shared_ptr<StoredTableNode> make_stored_table_node(const std::string& table_name, const std::optional<std::string>& alias) {
  const auto stored_table_node = std::make_shared<StoredTableNode>(table_name);
  stored_table_node->set_alias(alias);
  return stored_table_node;
}

std::shared_ptr<SortNode> make_sort_node(const OrderByDefinitions& order_by_definitions, const std::shared_ptr<AbstractLQPNode>& child) {
  const auto sort_node = std::make_shared<SortNode>(order_by_definitions);
  sort_node->set_left_child(child);
  return sort_node;
}

std::shared_ptr<JoinNode> make_join_node(const JoinMode join_mode, const LQPColumnReferencePair& join_column_references,
                                         const PredicateCondition predicate_condition, const std::shared_ptr<AbstractLQPNode>& left_child, const std::shared_ptr<AbstractLQPNode>& right_child) {
  const auto join_node = std::make_shared<JoinNode>(join_mode, join_column_references, predicate_condition);
  join_node->set_left_child(left_child);
  join_node->set_right_child(right_child);
  return join_node;
}

std::shared_ptr<JoinNode> make_join_node(const JoinMode join_mode, const std::shared_ptr<AbstractLQPNode>& left_child, const std::shared_ptr<AbstractLQPNode>& right_child) {
  const auto join_node = std::make_shared<JoinNode>(join_mode);
  join_node->set_left_child(left_child);
  join_node->set_right_child(right_child);
  return join_node;
}

std::shared_ptr<LimitNode> make_limit_node(const size_t num_rows, const std::shared_ptr<AbstractLQPNode>& child) {
  const auto limit_node = std::make_shared<LimitNode>(num_rows);
  limit_node->set_left_child(child);
  return limit_node;
}

std::shared_ptr<InsertNode> make_insert_node(const std::string& table_name, const std::shared_ptr<AbstractLQPNode>& child) {
  const auto insert_node = std::make_shared<InsertNode>(table_name);
  insert_node->set_left_child(child);
  return insert_node;
}

std::shared_ptr<UpdateNode> make_update_node(const std::string& table_name, const std::vector<std::shared_ptr<LQPExpression>>& column_expressions, const std::shared_ptr<AbstractLQPNode>& child) {
  const auto update_node = std::make_shared<UpdateNode>(table_name, column_expressions);
  update_node->set_left_child(child);
  return update_node;
}

std::shared_ptr<CreateViewNode> make_create_view_node(const std::string& view_name, std::shared_ptr<const AbstractLQPNode> lqp) {
  return std::make_shared<CreateViewNode>(view_name, lqp);
}

}  // namespace opossum