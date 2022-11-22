#include "order_dependency.hpp"

#include "expression/lqp_column_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"

namespace hyrise {

OrderDependency::OrderDependency(std::vector<std::shared_ptr<AbstractExpression>> init_determinants,
                                 std::vector<std::shared_ptr<AbstractExpression>> init_dependents)
    : determinants{std::move(init_determinants)}, dependents{std::move(init_dependents)} {
  Assert(!determinants.empty() && !dependents.empty(), "OrderDependency cannot be empty.");
}

bool OrderDependency::operator==(const OrderDependency& rhs) const {
  if (determinants.size() != rhs.determinants.size()) {
    return false;
  }

  for (auto expression_idx = size_t{0}; expression_idx < determinants.size(); ++expression_idx) {
    if (*determinants[expression_idx] != *rhs.determinants[expression_idx] ||
        *dependents[expression_idx] != *rhs.dependents[expression_idx]) {
      return false;
    }
  }

  return true;
}

bool OrderDependency::operator!=(const OrderDependency& rhs) const {
  return !(rhs == *this);
}

size_t OrderDependency::hash() const {
  auto hash = boost::hash_value(determinants.size());
  for (const auto& expression : determinants) {
    boost::hash_combine(hash, expression->hash());
  }

  for (const auto& expression : dependents) {
    boost::hash_combine(hash, expression->hash());
  }

  return hash;
}

std::ostream& operator<<(std::ostream& stream, const OrderDependency& od) {
  stream << "{";
  stream << od.determinants.at(0)->as_column_name();
  for (auto expression_idx = size_t{1}; expression_idx < od.determinants.size(); ++expression_idx) {
    stream << ", " << od.determinants[expression_idx]->as_column_name();
  }
  stream << "} orders {";
  stream << od.dependents.at(0)->as_column_name();
  for (auto expression_idx = size_t{1}; expression_idx < od.dependents.size(); ++expression_idx) {
    stream << ", " << od.dependents[expression_idx]->as_column_name();
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
