#include "execute_statement_node.hpp"

#include <sstream>

#include "expression/abstract_expression.hpp"
#include "expression/expression_utils.hpp"

namespace opossum {

ExecuteStatementNode::ExecuteStatementNode(const std::string& name, const std::vector<std::shared_ptr<AbstractExpression>>& parameters):
  BaseNonQueryNode(LQPNodeType::ExecuteStatement), name(name), parameters(parameters) {}

std::string ExecuteStatementNode::description() const {
  auto stream = std::ostringstream{};
  stream << "[Execute] '" << name << "' (";
  for (auto parameter_idx = size_t{0}; parameter_idx < parameters.size(); ++parameter_idx) {
    stream << parameters[parameter_idx]->as_column_name();
    if (parameter_idx + 1 < parameters.size()) {
      stream << ", ";
    }
  }
  stream << ")";

  return stream.str();
}

std::shared_ptr<AbstractLQPNode> ExecuteStatementNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return std::make_shared<ExecuteStatementNode>(name, parameters);
}

bool ExecuteStatementNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& execute_statement = static_cast<const ExecuteStatementNode&>(rhs);
  return name == execute_statement.name && expressions_equal(parameters, execute_statement.parameters);
}

}  // namespace opossum
