#pragma once

namespace opossum {

enum class ExpressionType {
  Value,
  Column,
  Placeholder,
  /*An identifier for a function, such as COUNT, MIN, MAX*/
  AggregateFunction,

  /*A subselect*/
  Subselect,

  /*Arithmetic operators*/
  Addition,
  Subtraction,
  Multiplication,
  Division,
  Modulo,
  Power,

  /*Logical operators*/
  Equals,
  NotEquals,
  LessThan,
  LessThanEquals,
  GreaterThan,
  GreaterThanEquals,
  Like,
  NotLike,
  And,
  Or,
  Between,
  Not,

  /*Set operators*/
  In,
  Exists,

  /*Others*/
  IsNull,
  IsNotNull,
  Case,
  Hint
};

template<typename ColumnReference>
class AbstractExpression {

};

}  // namespace opossum
