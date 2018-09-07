#include "alias_node.hpp"

#include <sstream>

#include "boost/algorithm/string/join.hpp"
#include "expression/expression_utils.hpp"

namespace opossum {

AliasNode::AliasNode(const std::vector<std::shared_ptr<AbstractExpression>>& expressions,
                     const std::vector<std::string>& aliases)
    : AbstractLQPNode(LQPNodeType::Alias), aliases(aliases), _expressions(expressions) {
  Assert(_expressions.size() == aliases.size(), "Specify a name for each Expression");
}

std::string AliasNode::description() const {
  std::stringstream stream;
  stream << "Alias [";
  for (auto column_id = ColumnID{0}; column_id < _expressions.size(); ++column_id) {
    if (_expressions[column_id]->as_column_name() == aliases[column_id]) {
      stream << aliases[column_id];
    } else {
      stream << _expressions[column_id]->as_column_name() << " AS " << aliases[column_id];
    }

    if (column_id + 1u < _expressions.size()) stream << ", ";
  }
  stream << "]";
  return stream.str();
}

const std::vector<std::shared_ptr<AbstractExpression>>& AliasNode::column_expressions() const { return _expressions; }

std::shared_ptr<AbstractLQPNode> AliasNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return std::make_shared<AliasNode>(expressions_copy_and_adapt_to_different_lqp(_expressions, node_mapping), aliases);
}

bool AliasNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& alias_node = static_cast<const AliasNode&>(rhs);
  return expressions_equal_to_expressions_in_different_lqp(_expressions, alias_node._expressions, node_mapping) &&
         aliases == alias_node.aliases;
}

}  // namespace opossum
