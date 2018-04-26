#include "expression_data_type.hpp"

namespace opossum {

bool is_invalid_arguments(const ExpressionDataTypeVariant& variant) {
  return variant.type() == typeid(ExpressionDataTypeInvalidArguments);
}

bool is_vacant(const ExpressionDataTypeVariant& variant) {
  return variant.type() == typeid(ExpressionDataTypeVacant);
}

}  // namespace opossum