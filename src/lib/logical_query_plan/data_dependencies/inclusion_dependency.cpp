#include "inclusion_dependency.hpp"

#include "expression/lqp_column_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"

namespace {

using namespace hyrise;  // NOLINT(build/namespaces)

std::shared_ptr<const AbstractLQPNode> original_node_from_expression(
    const std::shared_ptr<AbstractExpression>& expression) {
  Assert(expression->type == ExpressionType::LQPColumn, "InclusionDependency must reference expressions");
  const auto& column_expression = static_cast<LQPColumnExpression&>(*expression);
  const auto& original_node = column_expression.original_node.lock();
  Assert(original_node, "Expected node");
  return original_node;
}

}  // namespace

namespace hyrise {

InclusionDependency::InclusionDependency(std::vector<std::shared_ptr<AbstractExpression>> init_expressions,
                                         std::vector<std::shared_ptr<AbstractExpression>> init_included_expressions)
    : expressions{std::move(init_expressions)}, included_expressions{std::move(init_included_expressions)} {
  Assert(!expressions.empty(), "InclusionDependency cannot be empty.");
  Assert(expressions.size() == included_expressions.size(),
         "InclusionDependency expects same amount of including and included expressions.");

  if constexpr (HYRISE_DEBUG) {
    const auto& first_original_node = original_node_from_expression(included_expressions.front());
    for (const auto& expression : included_expressions) {
      const auto& original_node = original_node_from_expression(expression);
      Assert(original_node == first_original_node, "expressions must stem from same node.");
    }
  }
}

bool InclusionDependency::operator==(const InclusionDependency& rhs) const {
  if (expressions.size() != rhs.expressions.size()) {
    return false;
  }
  auto expression_count = expressions.size();

  for (auto expression_idx = size_t{0}; expression_idx < expression_count; ++expression_idx) {
    if (*expressions[expression_idx] != *rhs.expressions[expression_idx] ||
        *included_expressions[expression_idx] != *rhs.included_expressions[expression_idx]) {
      return false;
    }
  }

  return true;
}

bool InclusionDependency::operator!=(const InclusionDependency& rhs) const {
  return !(rhs == *this);
}

size_t InclusionDependency::hash() const {
  auto hash = boost::hash_value(expressions.size());
  for (const auto& expression : expressions) {
    boost::hash_combine(hash, expression->hash());
  }

  for (const auto& expression : included_expressions) {
    boost::hash_combine(hash, expression->hash());
  }

  return hash;
}

std::ostream& operator<<(std::ostream& stream, const InclusionDependency& ind) {
  stream << "{";
  stream << ind.included_expressions.at(0)->as_column_name();
  for (auto expression_idx = size_t{1}; expression_idx < ind.included_expressions.size(); ++expression_idx) {
    stream << ", " << ind.included_expressions[expression_idx]->as_column_name();
  }
  stream << "} in {";
  stream << ind.expressions.at(0)->as_column_name();
  for (auto expression_idx = size_t{1}; expression_idx < ind.expressions.size(); ++expression_idx) {
    stream << ", " << ind.expressions[expression_idx]->as_column_name();
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
