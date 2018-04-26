#pragma once

#include "boost/variant.hpp"
#include "all_type_variant.hpp"

namespace opossum {

struct ExpressionDataTypeInvalidArguments final {};
struct ExpressionDataTypeVacant final {};
//struct ExpressionDataTypeArray final {
//  boost::variant<DataType, ExpressionDataTypeVacant, ExpressionDataTypeInvalidArguments> element_type;
//};

using ExpressionDataTypeVariant = boost::variant<DataType, ExpressionDataTypeVacant, ExpressionDataTypeInvalidArguments>;

bool is_invalid_arguments(const ExpressionDataTypeVariant& variant);
bool is_vacant(const ExpressionDataTypeVariant& variant);

}  // namespace opossum
