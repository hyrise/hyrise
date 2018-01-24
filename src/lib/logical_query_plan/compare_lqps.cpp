#include "compare_lqps.hpp"

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

SemanticLQPCompare::SemanticLQPCompare(const std::shared_ptr<const AbstractLQPNode>& lhs, const std::shared_ptr<const AbstractLQPNode>& rhs): _lhs(lhs), _rhs(rhs) {

}

bool SemanticLQPCompare::operator()() {
  if (!_structural_traverse(_lhs, _rhs)) {
    return false;
  }

  return _semantical_traverse(_lhs, _rhs);
}

bool SemanticLQPCompare::_structural_traverse(const std::shared_ptr<const AbstractLQPNode>& lhs, const std::shared_ptr<const AbstractLQPNode>& rhs) {
  if (lhs == nullptr && rhs == nullptr) return true;
  if (lhs == nullptr || rhs == nullptr) return false;

  if (lhs->type() != rhs->type()) return false;

  // Checks number of columns AND names
  if (lhs->output_column_names() != rhs->output_column_names()) return false;

  _node_mapping[lhs] = rhs;

  return _structural_traverse(lhs->left_child(), rhs->left_child()) &&
  _structural_traverse(lhs->right_child(), rhs->right_child());
}

bool SemanticLQPCompare::_semantical_traverse(const std::shared_ptr<const AbstractLQPNode>& lhs, const std::shared_ptr<const AbstractLQPNode>& rhs) {
  if (!lhs) return false; // Implies !rhs

  auto semantically_equal = false;

  switch(lhs->type()) {
    case LQPNodeType::Aggregate: semantically_equal = _are_semantically_equal(std::dynamic_pointer_cast<const AggregateNode>(lhs), std::dynamic_pointer_cast<const AggregateNode>(rhs)); break;
    case LQPNodeType::CreateView: semantically_equal = _are_semantically_equal(std::dynamic_pointer_cast<const CreateViewNode>(lhs), std::dynamic_pointer_cast<const CreateViewNode>(rhs)); break;
    case LQPNodeType::Delete: semantically_equal = _are_semantically_equal(std::dynamic_pointer_cast<const DeleteNode>(lhs), std::dynamic_pointer_cast<const DeleteNode>(rhs)); break;
    case LQPNodeType::DropView: semantically_equal = _are_semantically_equal(std::dynamic_pointer_cast<const DropViewNode>(lhs), std::dynamic_pointer_cast<const DropViewNode>(rhs)); break;
    case LQPNodeType::DummyTable: semantically_equal = _are_semantically_equal(std::dynamic_pointer_cast<const DummyTableNode>(lhs), std::dynamic_pointer_cast<const DummyTableNode>(rhs)); break;
    case LQPNodeType::Insert: semantically_equal = _are_semantically_equal(std::dynamic_pointer_cast<const InsertNode>(lhs), std::dynamic_pointer_cast<const InsertNode>(rhs)); break;
    case LQPNodeType::Join: semantically_equal = _are_semantically_equal(std::dynamic_pointer_cast<const JoinNode>(lhs), std::dynamic_pointer_cast<const JoinNode>(rhs)); break;
    case LQPNodeType::Limit: semantically_equal = _are_semantically_equal(std::dynamic_pointer_cast<const LimitNode>(lhs), std::dynamic_pointer_cast<const LimitNode>(rhs)); break;
    case LQPNodeType::Predicate: semantically_equal = _are_semantically_equal(std::dynamic_pointer_cast<const PredicateNode>(lhs), std::dynamic_pointer_cast<const PredicateNode>(rhs)); break;
    case LQPNodeType::Projection: semantically_equal = _are_semantically_equal(std::dynamic_pointer_cast<const ProjectionNode>(lhs), std::dynamic_pointer_cast<const ProjectionNode>(rhs)); break;
    case LQPNodeType::Root: semantically_equal = _are_semantically_equal(std::dynamic_pointer_cast<const LogicalPlanRootNode>(lhs), std::dynamic_pointer_cast<const LogicalPlanRootNode>(rhs)); break;
    case LQPNodeType::ShowColumns: semantically_equal = _are_semantically_equal(std::dynamic_pointer_cast<const ShowColumnsNode>(lhs), std::dynamic_pointer_cast<const ShowColumnsNode>(rhs)); break;
    case LQPNodeType::ShowTables: semantically_equal = _are_semantically_equal(std::dynamic_pointer_cast<const ShowTablesNode>(lhs), std::dynamic_pointer_cast<const ShowTablesNode>(rhs)); break;
    case LQPNodeType::Sort: semantically_equal = _are_semantically_equal(std::dynamic_pointer_cast<const SortNode>(lhs), std::dynamic_pointer_cast<const SortNode>(rhs)); break;
    case LQPNodeType::StoredTable: semantically_equal = _are_semantically_equal(std::dynamic_pointer_cast<const StoredTableNode>(lhs), std::dynamic_pointer_cast<const StoredTableNode>(rhs)); break;
    case LQPNodeType::Update: semantically_equal = _are_semantically_equal(std::dynamic_pointer_cast<const UpdateNode>(lhs), std::dynamic_pointer_cast<const UpdateNode>(rhs)); break;
    case LQPNodeType::Union: semantically_equal = _are_semantically_equal(std::dynamic_pointer_cast<const UnionNode>(lhs), std::dynamic_pointer_cast<const UnionNode>(rhs)); break;
    case LQPNodeType::Validate: semantically_equal = _are_semantically_equal(std::dynamic_pointer_cast<const ValidateNode>(lhs), std::dynamic_pointer_cast<const ValidateNode>(rhs)); break;
    case LQPNodeType::Mock: semantically_equal = _are_semantically_equal(std::dynamic_pointer_cast<const MockNode>(lhs), std::dynamic_pointer_cast<const MockNode>(rhs)); break;
  }

  if (!semantically_equal) return false;

  if (!_semantical_traverse(lhs->left_child(), rhs->left_child())) return false;
  return _semantical_traverse(lhs->right_child(), rhs->right_child());
}

bool SemanticLQPCompare::_are_semantically_equal(const std::shared_ptr<const AggregateNode>& lhs, const std::shared_ptr<const AggregateNode>& rhs) {
  return _compare_expressions(lhs, lhs->aggregate_expressions(), rhs, rhs->aggregate_expressions()) && _compare_column_references(lhs, lhs->groupby_column_references(), rhs, rhs->groupby_column_references());
}

bool SemanticLQPCompare::_are_semantically_equal(const std::shared_ptr<const CreateViewNode>& lhs, const std::shared_ptr<const CreateViewNode>& rhs) {
  return lhs->view_name() == rhs->view_name() && SemanticLQPCompare{lhs->lqp(), rhs->lqp()}();
}

bool SemanticLQPCompare::_are_semantically_equal(const std::shared_ptr<const DeleteNode>& lhs, const std::shared_ptr<const DeleteNode>& rhs) {
  return lhs->table_name() == rhs->table_name();
}

bool SemanticLQPCompare::_are_semantically_equal(const std::shared_ptr<const DropViewNode>& lhs, const std::shared_ptr<const DropViewNode>& rhs) {
  return lhs->view_name() == rhs->view_name();
}

bool SemanticLQPCompare::_are_semantically_equal(const std::shared_ptr<const DummyTableNode>& lhs, const std::shared_ptr<const DummyTableNode>& rhs) {
  return true;
}

bool SemanticLQPCompare::_are_semantically_equal(const std::shared_ptr<const InsertNode>& lhs, const std::shared_ptr<const InsertNode>& rhs) {
  return lhs->table_name() == rhs->table_name();
}

bool SemanticLQPCompare::_are_semantically_equal(const std::shared_ptr<const JoinNode>& lhs, const std::shared_ptr<const JoinNode>& rhs) {
  if (lhs->join_mode() != rhs->join_mode() || lhs->scan_type() != rhs->scan_type()) return false;
  if (lhs->join_column_references().has_value() != rhs->join_column_references().has_value()) return false;

  if (!lhs->join_column_references().has_value()) return true;

  return _compare_column_references(lhs, lhs->join_column_references()->first, rhs, rhs->join_column_references()->first) &&
  _compare_column_references(lhs, lhs->join_column_references()->second, rhs, rhs->join_column_references()->second);
}

bool SemanticLQPCompare::_are_semantically_equal(const std::shared_ptr<const LimitNode>& lhs, const std::shared_ptr<const LimitNode>& rhs) {
  return lhs->num_rows() == rhs->num_rows();
}

bool SemanticLQPCompare::_are_semantically_equal(const std::shared_ptr<const PredicateNode>& lhs, const std::shared_ptr<const PredicateNode>& rhs) {
  if (!_compare_column_references(lhs, lhs->column_reference(), rhs, rhs->column_reference())) return false;
  if (lhs->scan_type() != rhs->scan_type()) return false;
  if (is_lqp_column_reference(lhs->value()) != is_lqp_column_reference(rhs->value())) return false;
  if (is_lqp_column_reference(lhs->value())) {
    if (_compare_column_references(lhs, boost::get<LQPColumnReference>(lhs->value()), rhs, boost::get<LQPColumnReference>(rhs->value()))) return false;
  } else {
    if (lhs->value() != rhs->value()) return false;
  }

  return lhs->value2() != rhs->value2();
}

bool SemanticLQPCompare::_are_semantically_equal(const std::shared_ptr<const ProjectionNode>& lhs, const std::shared_ptr<const ProjectionNode>& rhs) {
  return _compare_expressions(lhs, lhs->column_expressions(), rhs, rhs->column_expressions());
}

bool SemanticLQPCompare::_are_semantically_equal(const std::shared_ptr<const LogicalPlanRootNode>& lhs, const std::shared_ptr<const LogicalPlanRootNode>& rhs) {
  return true;
}

bool SemanticLQPCompare::_are_semantically_equal(const std::shared_ptr<const ShowColumnsNode>& lhs, const std::shared_ptr<const ShowColumnsNode>& rhs) {
  return lhs->table_name() == rhs->table_name();
}

bool SemanticLQPCompare::_are_semantically_equal(const std::shared_ptr<const ShowTablesNode>& lhs, const std::shared_ptr<const ShowTablesNode>& rhs) {
  return true;
}

bool SemanticLQPCompare::_are_semantically_equal(const std::shared_ptr<const SortNode>& lhs, const std::shared_ptr<const SortNode>& rhs) {
  if (lhs->order_by_definitions().size() != rhs->order_by_definitions().size()) return false;

  for (size_t definition_idx = 0; definition_idx < rhs->order_by_definitions().size(); ++definition_idx) {
    if (lhs->order_by_definitions()[definition_idx].order_by_mode != rhs->order_by_definitions()[definition_idx].order_by_mode) return false;
    if (_compare_column_references(lhs, lhs->order_by_definitions()[definition_idx].column_reference, rhs, rhs->order_by_definitions()[definition_idx].column_reference)) return false;
  }

  return true;
}

bool SemanticLQPCompare::_are_semantically_equal(const std::shared_ptr<const StoredTableNode>& lhs, const std::shared_ptr<const StoredTableNode>& rhs) {
  return lhs->table_name() == rhs->table_name();
}

bool SemanticLQPCompare::_are_semantically_equal(const std::shared_ptr<const UpdateNode>& lhs, const std::shared_ptr<const UpdateNode>& rhs) {
  return lhs->table_name() == rhs->table_name() && _compare_expressions(lhs, lhs->column_expressions(), rhs, rhs->column_expressions());
}

bool SemanticLQPCompare::_are_semantically_equal(const std::shared_ptr<const UnionNode>& lhs, const std::shared_ptr<const UnionNode>& rhs) {
  return lhs->union_mode() == rhs->union_mode();
}

bool SemanticLQPCompare::_are_semantically_equal(const std::shared_ptr<const ValidateNode>& lhs, const std::shared_ptr<const ValidateNode>& rhs) {
  return true;
}

bool SemanticLQPCompare::_are_semantically_equal(const std::shared_ptr<const MockNode>& lhs, const std::shared_ptr<const MockNode>& rhs) {
  return lhs->constructor_arguments() == rhs->constructor_arguments();
//  if (lhs->constructor_arguments().which() != rhs->constructor_arguments().which()) return false;
//
//  if (lhs->constructor_arguments().type() == typeid(MockNode::ColumnDefinitions)) {
//    return boost::get<MockNode::ColumnDefinitions>(lhs->constructor_arguments()) ==
//  } else if (lhs->constructor_arguments().type() == typeid(std::shared_ptr<TableStatistics>)) {
//
//  } else {
//    Fail("Constructor arguments comparison not implemented for this type.")
//  }
}

bool SemanticLQPCompare::_compare_expressions(const std::shared_ptr<const AbstractLQPNode>&lqp_left, const std::vector<std::shared_ptr<LQPExpression>>& expressions_left,
                          const std::shared_ptr<const AbstractLQPNode>&lqp_right, const std::vector<std::shared_ptr<LQPExpression>>& expressions_right) const {
  if (expressions_left.size() != expressions_right.size()) return false;

  for (size_t expression_idx = 0; expression_idx < expressions_left.size(); ++expression_idx) {
    if (!_compare_expressions(lqp_left, expressions_left[expression_idx], lqp_right, expressions_right[expression_idx])) return false;
  }

  return true;
}

bool SemanticLQPCompare::_compare_expressions(const std::shared_ptr<const AbstractLQPNode>&lqp_left, const std::shared_ptr<const LQPExpression>& expression_left,
                          const std::shared_ptr<const AbstractLQPNode>&lqp_right, const std::shared_ptr<const LQPExpression>& expression_right) const {
  if (static_cast<bool>(expression_left) != static_cast<bool>(expression_right)) return false;
  if (*expression_left == *expression_right) return true;
  if (expression_left->type() != expression_right->type()) return false;
  if (expression_left->aggregate_function_arguments().size() != expression_right->aggregate_function_arguments().size()) return false;

  const auto type = expression_left->type();

  if (type == ExpressionType::Column) {
    return _compare_column_references(lqp_left, expression_left->column_reference(), lqp_right, expression_right->column_reference());
  }

  for (size_t arg_idx = 0; arg_idx < expression_left->aggregate_function_arguments().size(); ++arg_idx) {
    if (!_compare_expressions(lqp_left, expression_left->aggregate_function_arguments()[arg_idx], lqp_right, expression_right->aggregate_function_arguments()[arg_idx])) return false;
  }

  if (!_compare_expressions(lqp_left, expression_left->left_child(), lqp_right, expression_right->left_child())) return false;
  if (!_compare_expressions(lqp_right, expression_left->right_child(), lqp_right, expression_right->right_child())) return false;

  return true;
}

bool SemanticLQPCompare::_compare_column_references(const std::shared_ptr<const AbstractLQPNode>&lqp_left, const std::vector<LQPColumnReference>& column_references_left,
                                const std::shared_ptr<const AbstractLQPNode>&lqp_right, const std::vector<LQPColumnReference>& column_references_right) const {
  if (column_references_left.size() != column_references_right.size()) return false;
  for (size_t column_reference_idx = 0; column_reference_idx < column_references_left.size(); ++column_reference_idx) {
    if (!_compare_column_references(lqp_left, column_references_left[column_reference_idx], lqp_right, column_references_right[column_reference_idx])) return false;
  }
  return true;
}

bool SemanticLQPCompare:: _compare_column_references(const std::shared_ptr<const AbstractLQPNode>&lqp_left, const LQPColumnReference& column_reference_left,
                                const std::shared_ptr<const AbstractLQPNode>&lqp_right, const LQPColumnReference& column_reference_right) const {
  // We just need a temporary ColumnReference which won't be used to manipulate nodes, promised.
  const auto mutable_lqp_left = std::const_pointer_cast<AbstractLQPNode>(lqp_left);
  const auto mutable_lqp_right = std::const_pointer_cast<AbstractLQPNode>(lqp_right);

  return AbstractLQPNode::adapt_column_reference_to_different_lqp(column_reference_right, mutable_lqp_left, mutable_lqp_right) == column_reference_right;
}

bool lqp_node_types_equal(const std::shared_ptr<const AbstractLQPNode>& lhs,
                          const std::shared_ptr<const AbstractLQPNode>& rhs) {
  if (lhs == nullptr && rhs == nullptr) return true;
  if (lhs == nullptr) return false;
  if (rhs == nullptr) return false;

  if (lhs->type() != rhs->type()) return false;
  return lqp_node_types_equal(lhs->left_child(), rhs->left_child()) &&
  lqp_node_types_equal(lhs->right_child(), rhs->right_child());
}

}