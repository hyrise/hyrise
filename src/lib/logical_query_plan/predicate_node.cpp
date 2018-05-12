#include "predicate_node.hpp"

#include <memory>
#include <optional>
#include <sstream>
#include <string>

#include "constant_mappings.hpp"
#include "expression/expression_utils.hpp"
#include "optimizer/table_statistics.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

PredicateNode::PredicateNode(const std::shared_ptr<AbstractExpression>& predicate): AbstractLQPNode(LQPNodeType::Predicate), predicate(predicate) {}

std::string PredicateNode::description() const {
  std::stringstream stream;
  stream << "[Predicate] " << predicate->as_column_name();
  return stream.str();
}

std::shared_ptr<AbstractLQPNode> PredicateNode::_shallow_copy_impl(LQPNodeMapping & node_mapping) const {
  return std::make_shared<PredicateNode>(expression_copy_and_adapt_to_different_lqp(*predicate, node_mapping));
}

bool PredicateNode::_shallow_equals_impl(const AbstractLQPNode& rhs, const LQPNodeMapping & node_mapping) const {
  const auto& predicate_node = static_cast<const PredicateNode&>(rhs);
  return expression_equal_to_expression_in_different_lqp(*predicate, *predicate_node.predicate, node_mapping);
}

}  // namespace opossum
