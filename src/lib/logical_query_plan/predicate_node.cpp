#include "predicate_node.hpp"

#include <memory>
#include <optional>
#include <sstream>
#include <string>

#include "constant_mappings.hpp"
#include "expression/between_expression.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/expression_utils.hpp"
#include "expression/lqp_column_expression.hpp"
#include "expression/value_expression.hpp"
#include "operators/operator_scan_predicate.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

PredicateNode::PredicateNode(const std::shared_ptr<AbstractExpression>& predicate)
    : AbstractLQPNode(LQPNodeType::Predicate, {predicate}) {}

std::string PredicateNode::description() const {
  std::stringstream stream;
  stream << "[Predicate] " << predicate()->as_column_name();
  return stream.str();
}

std::shared_ptr<AbstractExpression> PredicateNode::predicate() const { return node_expressions[0]; }

size_t PredicateNode::_shallow_hash() const { return boost::hash_value(scan_type); }

std::shared_ptr<AbstractLQPNode> PredicateNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return std::make_shared<PredicateNode>(expression_copy_and_adapt_to_different_lqp(*predicate(), node_mapping));
}

bool PredicateNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& predicate_node = static_cast<const PredicateNode&>(rhs);
  const auto equal =
      expression_equal_to_expression_in_different_lqp(*predicate(), *predicate_node.predicate(), node_mapping);

  return equal;
}

}  // namespace opossum
