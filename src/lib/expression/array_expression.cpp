#include "array_expression.hpp"

#include <sstream>

#include "expression_utils.hpp"
#include "utils/assert.hpp"

namespace opossum {

ArrayExpression::ArrayExpression(const std::vector<std::shared_ptr<AbstractExpression>>& elements):
AbstractExpression(ExpressionType::Array, elements){

}

DataType ArrayExpression::data_type() const {
  Fail("An ArrayExpression doesn't have a single type, each of its elements might have a different type");
}

bool ArrayExpression::is_nullable() const {
  if (elements().empty()) return false;

  const auto nullable = elements().front()->is_nullable();
  Assert(std::all_of(elements().begin(), elements().end(), [&](const auto& value) { return value->is_nullable() == nullable;}), "Nullability of Array elements is inconsistent");

  return nullable;
}

const std::vector<std::shared_ptr<AbstractExpression>>& ArrayExpression::elements() const {
  return arguments;
}

std::optional<DataType> ArrayExpression::common_element_data_type() const {
  if (elements().empty()) return std::nullopt;

  auto data_type = elements().front()->data_type();

  for (auto element_idx = size_t{1}; element_idx < elements().size(); ++element_idx) {
    data_type = expression_common_type(data_type, elements()[element_idx]->data_type());
  }

  return data_type;
}

std::shared_ptr<AbstractExpression> ArrayExpression::deep_copy() const {
    return std::make_shared<ArrayExpression>(expressions_copy(arguments));
}

std::string ArrayExpression::as_column_name() const {
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
