#include "plan_column_definition.hpp"

#include <sstream>

#include "expression/abstract_expression.hpp"

namespace opossum {

PlanColumnDefinition::PlanColumnDefinition(const std::shared_ptr<AbstractExpression>& expression, const std::optional<std::string>& alias):
  expression(expression), alias(alias)
{
}

std::string PlanColumnDefinition::description() const {
  std::stringstream stream;
  stream << expression->description();
  if (alias) {
    stream << " AS " << *alias;
  }
  return stream.str();
}

}  // namespace opossum
