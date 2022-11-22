#include "inclusion_dependency.hpp"

#include "expression/lqp_column_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"

namespace {

using namespace hyrise;  // NOLINT(build/namespaces)

std::shared_ptr<const AbstractLQPNode> original_node_from_expression(
    const std::shared_ptr<AbstractExpression>& expression) {
  Assert(expression->type == ExpressionType::LQPColumn, "InclusionDependency must reference columns");
  const auto& column_expression = static_cast<LQPColumnExpression&>(*expression);
  const auto& original_node = column_expression.original_node.lock();
  Assert(original_node, "Expected node");
  return original_node;
}

}  // namespace

namespace hyrise {

InclusionDependency::InclusionDependency(std::vector<std::shared_ptr<AbstractExpression>> init_determinants,
                                         std::vector<std::shared_ptr<AbstractExpression>> init_dependents)
    : determinants{std::move(init_determinants)}, dependents{std::move(init_dependents)} {
  Assert(!determinants.empty(), "InclusionDependency cannot be empty.");
  Assert(dependents.size() == determinants.size(),
         "InclusionDependency expects same amount of determinant and depedent columns.");

  if constexpr (HYRISE_DEBUG) {
    const auto& first_original_node = original_node_from_expression(dependents.front());
    for (const auto& expression : dependents) {
      const auto& original_node = original_node_from_expression(expression);
      Assert(original_node == first_original_node, "Columns must stem from same node.");
    }
  }
}

bool InclusionDependency::operator==(const InclusionDependency& rhs) const {
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

bool InclusionDependency::operator!=(const InclusionDependency& rhs) const {
  return !(rhs == *this);
}

size_t InclusionDependency::hash() const {
  auto hash = boost::hash_value(determinants.size());
  for (const auto& expression : determinants) {
    boost::hash_combine(hash, expression->hash());
  }

  for (const auto& expression : dependents) {
    boost::hash_combine(hash, expression->hash());
  }

  return hash;
}

std::ostream& operator<<(std::ostream& stream, const InclusionDependency& ind) {
  stream << "{";
  stream << ind.dependents.at(0)->as_column_name();
  for (auto expression_idx = size_t{1}; expression_idx < ind.dependents.size(); ++expression_idx) {
    stream << ", " << ind.dependents[expression_idx]->as_column_name();
  }
  stream << "} IN {";
  stream << ind.determinants.at(0)->as_column_name();
  for (auto expression_idx = size_t{1}; expression_idx < ind.determinants.size(); ++expression_idx) {
    stream << ", " << ind.determinants[expression_idx]->as_column_name();
  }
  stream << "}";
  return stream;
}

}  // namespace hyrise

namespace std {

size_t hash<hyrise::InclusionDependency>::operator()(const hyrise::InclusionDependency& ind) const {
  return ind.hash();
}

}  // namespace std
