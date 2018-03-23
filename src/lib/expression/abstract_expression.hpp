#pragma once

#include <memory>

#include "types.hpp"

namespace opossum {

enum class ExpressionType {
  Aggregate, Arithmetic, Case, Column, Exists, Function, In, Logical, Null, Predicate, Select, Value, ValuePlaceholder
};

class AbstractExpression : public std::enable_shared_from_this<AbstractExpression> {
 public:
  explicit AbstractExpression(const ExpressionType type): type(type) {}
  virtual ~AbstractExpression() = default;

  virtual bool deep_equals(const AbstractExpression& expression) const = 0;
  virtual std::shared_ptr<AbstractExpression> deep_copy() const = 0;

  /**
   * Used for creating the complete Expression that created a Column
   * @return `B`                    if `this` is a ColumnExpression pointing to a Column defined by
   *                                non-ColumnExpression `B`
   *          shared_from_this()    otherwise
   */
  virtual std::shared_ptr<AbstractExpression> deep_resolve_column_expressions() = 0;

  const ExpressionType type;
};


//if (arguments.size() != aggregate_expression.arguments.size()) return false;

//for (auto argument_idx = size_t{0}; argument_idx < arguments.size(); ++argument_idx) {
//if (!arguments[argument_idx]->deep_equals(*aggregate_expression.arguments[argument_idx])) return false;
//}

//return true;

}  // namespace opossum
