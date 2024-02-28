#include "alias_node.hpp"

#include <cstddef>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include <boost/container_hash/hash.hpp>

#include "expression/abstract_expression.hpp"
#include "expression/expression_utils.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/data_dependencies/unique_column_combination.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

AliasNode::AliasNode(const std::vector<std::shared_ptr<AbstractExpression>>& expressions,
                     const std::vector<std::string>& init_aliases)
    : AbstractLQPNode(LQPNodeType::Alias, expressions), aliases(init_aliases) {
  Assert(expressions.size() == aliases.size(), "Number of expressions and number of aliases has to be equal.");
}

std::string AliasNode::description(const DescriptionMode mode) const {
  const auto expression_mode = _expression_description_mode(mode);
  auto stream = std::stringstream{};
  stream << "[Alias] ";
  for (auto column_id = ColumnID{0}; column_id < node_expressions.size(); ++column_id) {
    if (node_expressions[column_id]->description(expression_mode) == aliases[column_id]) {
      stream << aliases[column_id];
    } else {
      stream << node_expressions[column_id]->description(expression_mode) << " AS " << aliases[column_id];
    }

    if (column_id + size_t{1} < node_expressions.size()) {
      stream << ", ";
    }
  }
  return stream.str();
}

std::vector<std::shared_ptr<AbstractExpression>> AliasNode::output_expressions() const {
  return node_expressions;
}

UniqueColumnCombinations AliasNode::unique_column_combinations() const {
  return _forward_left_unique_column_combinations();
}

OrderDependencies AliasNode::order_dependencies() const {
  return _forward_left_order_dependencies();
}

size_t AliasNode::_on_shallow_hash() const {
  auto hash = size_t{0};
  for (const auto& alias : aliases) {
    boost::hash_combine(hash, alias);
  }
  return hash;
}

std::shared_ptr<AbstractLQPNode> AliasNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return std::make_shared<AliasNode>(expressions_copy_and_adapt_to_different_lqp(node_expressions, node_mapping),
                                     aliases);
}

bool AliasNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& alias_node = static_cast<const AliasNode&>(rhs);
  return expressions_equal_to_expressions_in_different_lqp(node_expressions, alias_node.node_expressions,
                                                           node_mapping) &&
         aliases == alias_node.aliases;
}

}  // namespace hyrise
