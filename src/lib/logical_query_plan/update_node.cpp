#include "update_node.hpp"

#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "lqp_expression.hpp"
#include "utils/assert.hpp"

namespace opossum {

UpdateNode::UpdateNode(const std::string& table_name,
                       const std::vector<std::shared_ptr<LQPExpression>>& column_expressions)
    : AbstractLQPNode(LQPNodeType::Update), _table_name(table_name), _column_expressions(column_expressions) {}

std::shared_ptr<AbstractLQPNode> UpdateNode::_deep_copy_impl() const {
  std::vector<std::shared_ptr<LQPExpression>> column_expressions;
  column_expressions.reserve(_column_expressions.size());
  for (const auto& expression : _column_expressions) {
    column_expressions.emplace_back(expression->deep_copy());
  }

  return std::make_shared<UpdateNode>(_table_name, std::move(_column_expressions));
}

std::string UpdateNode::description() const {
  std::ostringstream desc;

  desc << "[Update] Table: '" << _table_name << "'";

  return desc.str();
}

bool UpdateNode::subtree_is_read_only() const { return false; }

const std::vector<std::shared_ptr<LQPExpression>>& UpdateNode::column_expressions() const { return _column_expressions; }

const std::string& UpdateNode::table_name() const { return _table_name; }

}  // namespace opossum
