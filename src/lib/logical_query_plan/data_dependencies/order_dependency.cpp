#include "order_dependency.hpp"

#include "expression/lqp_column_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"

namespace hyrise {

OrderDependency::OrderDependency(std::vector<std::shared_ptr<AbstractExpression>> init_expressions,
                                 std::vector<std::shared_ptr<AbstractExpression>> init_ordered_expessions)
    : expressions{std::move(init_expressions)}, ordered_expressions{std::move(init_ordered_expessions)} {
  Assert(!expressions.empty() && !ordered_expressions.empty(), "OrderDependency cannot be empty.");
}

bool OrderDependency::operator==(const OrderDependency& rhs) const {
  if (expressions.size() != rhs.expressions.size() || ordered_expressions.size() != rhs.ordered_expressions.size()) {
    return false;
  }

  for (auto expression_idx = size_t{0}; expression_idx < expressions.size(); ++expression_idx) {
    if (*expressions[expression_idx] != *rhs.expressions[expression_idx]) {
      return false;
    }
  }

  for (auto expression_idx = size_t{0}; expression_idx < ordered_expressions.size(); ++expression_idx) {
    if (*ordered_expressions[expression_idx] != *rhs.ordered_expressions[expression_idx]) {
      return false;
    }
  }

  return true;
}

bool OrderDependency::operator!=(const OrderDependency& rhs) const {
  return !(rhs == *this);
}

size_t OrderDependency::hash() const {
  auto hash = boost::hash_value(expressions.size());
  for (const auto& expression : expressions) {
    boost::hash_combine(hash, expression->hash());
  }

  boost::hash_combine(hash, ordered_expressions.size());
  for (const auto& expression : ordered_expressions) {
    boost::hash_combine(hash, expression->hash());
  }

  return hash;
}

std::ostream& operator<<(std::ostream& stream, const OrderDependency& od) {
  stream << "{";
  stream << od.expressions[0]->as_column_name();
  for (auto expression_idx = size_t{1}; expression_idx < od.expressions.size(); ++expression_idx) {
    stream << ", " << od.expressions[expression_idx]->as_column_name();
  }
  stream << "} |-> {";
  stream << od.ordered_expressions[0]->as_column_name();
  for (auto expression_idx = size_t{1}; expression_idx < od.ordered_expressions.size(); ++expression_idx) {
    stream << ", " << od.ordered_expressions[expression_idx]->as_column_name();
  }
  stream << "}";
  return stream;
}

}  // namespace hyrise

namespace std {

size_t hash<hyrise::OrderDependency>::operator()(const hyrise::OrderDependency& od) const {
  return od.hash();
}

}  // namespace std
