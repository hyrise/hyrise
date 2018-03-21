#pragma once

namespace opossum {

enum class ExpressionType {
  Aggregate,
  AllParameterVariant,
  Arithmetic,
  Case,
  Exists,
  Function,
  In,
  Logical,
  Select,
  Arithmetic,
  Subselect
};

template<typename ColumnReference>
class AbstractExpression {

};

}  // namespace opossum
