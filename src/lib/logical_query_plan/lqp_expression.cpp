#include "lqp_expression.hpp"

#include "utils/assert.hpp"

namespace opossum {

const ColumnOrigin& LQPExpression::column_origin() const {
  DebugAssert(_column_origin, "Expression " + expression_type_to_string.at(_type) + " does not have a ColumnOrigin");
  return *_column_origin;
}

}