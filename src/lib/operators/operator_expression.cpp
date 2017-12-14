#include "operator_expression.hpp"

#include "utils/assert.hpp"

namespace opossum {

ColumnID OperatorExpression::column_id() const {
  DebugAssert(_column_id, "Expression " + expression_type_to_string.at(_type) + " does not have a ColumnID");
  return *_column_id;
}

}