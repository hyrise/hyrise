#pragma once

#include <optional>

#include "expression.hpp"
#include "types.hpp"

namespace opossum {

class OperatorExpression : public Expression {
 public:
  ColumnID column_id() const;

 private:
  std::optional<ColumnID> _column_id;
};

}