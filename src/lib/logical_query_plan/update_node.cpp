#include "update_node.hpp"

#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "expression/expression_utils.hpp"
#include "utils/assert.hpp"

namespace opossum {

UpdateNode::UpdateNode(const std::string& table_name,
                       const std::vector<std::shared_ptr<AbstractExpression>>& column_expressions)
    : AbstractLQPNode(LQPNodeType::Update), _table_name(table_name), _column_expressions(column_expressions) {}

std::string UpdateNode::description() const {
  std::ostringstream desc;

  desc << "[Update] Table: '" << _table_name << "'";
  desc << " Columns: " << expression_column_names(_column_expressions);

  return desc.str();
}

const std::vector<std::shared_ptr<AbstractExpression>>& UpdateNode::column_expressions() const {
  return _column_expressions;
}

const std::string& UpdateNode::table_name() const { return _table_name; }

std::shared_ptr<AbstractLQPNode> UpdateNode::_shallow_copy_impl(LQPNodeMapping & node_mapping) const {
  return UpdateNode::make(_table_name, expressions_copy_and_adapt_to_different_lqp(_column_expressions, node_mapping));
}

bool UpdateNode::_shallow_equals_impl(const AbstractLQPNode& rhs, const LQPNodeMapping & node_mapping) const {
  const auto& update_node_rhs = static_cast<const UpdateNode&>(rhs);
  return _table_name == update_node_rhs._table_name && expressions_equal_to_expressions_in_different_lqp(_column_expressions, update_node_rhs._column_expressions, node_mapping);
}

}  // namespace opossum
