#include "list_expression.hpp"

#include <sstream>

#include "expression_utils.hpp"
#include "utils/assert.hpp"

namespace opossum {

ListExpression::ListExpression(const std::vector<std::shared_ptr<AbstractExpression>>& elements)
    : AbstractExpression(ExpressionType::List, elements) {}

DataType ListExpression::data_type() const {
  Fail("An ListExpression doesn't have a single type, each of its elements might have a different type");
}

const std::vector<std::shared_ptr<AbstractExpression>>& ListExpression::elements() const { return arguments; }

std::shared_ptr<AbstractExpression> ListExpression::deep_copy() const {
  return std::make_shared<ListExpression>(expressions_deep_copy(arguments));
}

std::string ListExpression::as_column_name() const {
  return std::string{"("} + expression_column_names(arguments) + ")";
}

}  // namespace opossum
