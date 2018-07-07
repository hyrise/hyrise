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

bool ListExpression::is_nullable() const {
  return std::any_of(elements().begin(), elements().end(), [&](const auto& value) { return value->is_nullable(); });
}

const std::vector<std::shared_ptr<AbstractExpression>>& ListExpression::elements() const { return arguments; }

std::shared_ptr<AbstractExpression> ListExpression::deep_copy() const {
  return std::make_shared<ListExpression>(expressions_copy(arguments));
}

std::string ListExpression::as_column_name() const {
  std::stringstream stream;
  stream << "(";
  for (auto element_idx = size_t{0}; element_idx < elements().size(); ++element_idx) {
    stream << elements()[element_idx]->as_column_name();
    if (element_idx + 1 < elements().size()) stream << ", ";
  }

  stream << ")";
  return stream.str();
}

}  // namespace opossum
