#include "list_expression.hpp"

#include <sstream>

#include "expression_utils.hpp"
#include "utils/assert.hpp"

namespace hyrise {

ListExpression::ListExpression(const std::vector<std::shared_ptr<AbstractExpression>>& elements)
    : AbstractExpression(ExpressionType::List, elements) {}

DataType ListExpression::data_type() const {
  Fail("A ListExpression doesn't have a single type, each of its elements might have a different type");
}

const std::vector<std::shared_ptr<AbstractExpression>>& ListExpression::elements() const {
  return arguments;
}

std::shared_ptr<AbstractExpression> ListExpression::_on_deep_copy(
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const {
  return std::make_shared<ListExpression>(expressions_deep_copy(arguments, copied_ops));
}

std::string ListExpression::description(const DescriptionMode mode) const {
  return std::string("(") + expression_descriptions(arguments, mode) + ")";
}

bool ListExpression::_shallow_equals(const AbstractExpression& expression) const {
  DebugAssert(dynamic_cast<const ListExpression*>(&expression),
              "Different expression type should have been caught by AbstractExpression::operator==");
  return true;
}

}  // namespace hyrise
