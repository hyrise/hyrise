#include "update_node.hpp"

#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "optimizer/expression.hpp"
#include "utils/assert.hpp"

namespace opossum {

class Expression;

UpdateNode::UpdateNode(const std::string& table_name,
                       const std::vector<std::shared_ptr<Expression>>& column_expressions)
    : AbstractASTNode(ASTNodeType::Update), _table_name(table_name), _column_expressions(column_expressions) {}

std::string UpdateNode::description() const {
  std::ostringstream desc;

  desc << "[Update] Table: '" << _table_name << "'";

  return desc.str();
}

bool UpdateNode::subtree_is_read_only() const { return false; }

const std::vector<std::shared_ptr<Expression>>& UpdateNode::column_expressions() const { return _column_expressions; }

const std::string& UpdateNode::table_name() const { return _table_name; }

std::shared_ptr<AbstractASTNode> UpdateNode::clone_subtree() const {
  auto clone = _clone_without_subclass_members<decltype(*this)>();
  for (auto& expression : clone->_column_expressions) {
    expression = std::make_shared<Expression>(*expression);
  }
  return clone;
}

}  // namespace opossum
