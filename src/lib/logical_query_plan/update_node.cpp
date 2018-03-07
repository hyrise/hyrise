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

std::shared_ptr<AbstractLQPNode> UpdateNode::_deep_copy_impl(
    const std::shared_ptr<AbstractLQPNode>& copied_left_input,
    const std::shared_ptr<AbstractLQPNode>& copied_right_input) const {
  std::vector<std::shared_ptr<LQPExpression>> column_expressions(_column_expressions.size());
  column_expressions.reserve(_column_expressions.size());

  for (const auto& expression : column_expressions) {
    column_expressions.emplace_back(
        adapt_expression_to_different_lqp(expression->deep_copy(), left_input(), copied_left_input));
  }

  return UpdateNode::make(_table_name, column_expressions);
}

std::string UpdateNode::description() const {
  std::ostringstream desc;

  desc << "[Update] Table: '" << _table_name << "'";

  if (!_column_expressions.empty()) {
    desc << ", Columns: ";
    std::vector<std::string> verbose_column_names;
    if (left_input()) {
      verbose_column_names = left_input()->get_verbose_column_names();
    }

    for (size_t column_idx = 0; column_idx < _column_expressions.size(); ++column_idx) {
      desc << _column_expressions[column_idx]->to_string(verbose_column_names);
      if (column_idx + 1 < _column_expressions.size()) {
        desc << ", ";
      }
    }
  }

  return desc.str();
}

bool UpdateNode::subplan_is_read_only() const { return false; }

const std::vector<std::shared_ptr<LQPExpression>>& UpdateNode::column_expressions() const {
  return _column_expressions;
}

const std::string& UpdateNode::table_name() const { return _table_name; }

bool UpdateNode::shallow_equals(const AbstractLQPNode& rhs) const {
  Assert(rhs.type() == type(), "Can only compare nodes of the same type()");
  const auto& update_node = static_cast<const UpdateNode&>(rhs);

  Assert(left_input() && rhs.left_input(), "Can't compare column references without inputs");
  return _table_name == update_node._table_name &&
         _equals(*left_input(), _column_expressions, *update_node.left_input(), update_node._column_expressions);
}
}  // namespace opossum
