#pragma once

#include <optional>

#include "expression.hpp"
#include "logical_query_plan/column_origin.hpp"

namespace opossum {

class LQPExpression : public Expression {
 public:
  const ColumnOrigin& column_origin() const;

 private:
  std::optional<ColumnOrigin> _column_origin;
};

}