#pragma once

#include <memory>
#include <vector>

#include "boost/variant.hpp"

#include "types.hpp"
#include "all_type_variant.hpp"
#include "null_value.hpp"

namespace opossum {

class AbstractExpression;
class BaseColumn;
class ArithmeticExpression;
class BinaryPredicateExpression;
class PredicateExpression;
class PQPSelectExpression;
class Chunk;

struct BaseColumnMaterialization {
  virtual ~BaseColumnMaterialization() = default;
};

template<typename T>
struct ColumnMaterialization : public BaseColumnMaterialization {
  std::optional<std::vector<bool>> nulls;
  std::vector<T> values;
};

class ExpressionEvaluator final {
 public:
  template<typename T> using NullableValues = std::pair<std::vector<T>, std::vector<bool>>;
  template<typename T> using NonNullableValues = std::vector<T>;

  template<typename T> using ExpressionResult = boost::variant<
    NullableValues<T>,
    NonNullableValues<T>,
    T,
    NullValue
  >;

  explicit ExpressionEvaluator(const std::shared_ptr<const Chunk>& chunk);

  void _ensure_column_materialization(const ColumnID column_id);

  std::shared_ptr<BaseColumn> evaluate_expression_to_column(const AbstractExpression& expression);

  template<typename T>
  ExpressionResult<T> evaluate_expression(const AbstractExpression& expression);

  template<typename T>
  ExpressionResult<T> evaluate_arithmetic_expression(const ArithmeticExpression& expression);

  template<typename T>
  ExpressionResult<T> evaluate_binary_predicate_expression(const BinaryPredicateExpression& expression);

  template<typename T>
  ExpressionResult<T> evaluate_select_expression(const PQPSelectExpression& expression);

  template<typename T, typename OperatorFunctor>
  ExpressionResult<T> evaluate_binary_expression(const AbstractExpression& left_operand,
                                                        const AbstractExpression& right_operand,
                                                        const OperatorFunctor &functor);
  template<typename ResultDataType,
           typename LeftOperandDataType,
           typename RightOperandDataType,
           typename Functor>
  ExpressionResult<ResultDataType> evaluate_binary_operator(const ExpressionResult<LeftOperandDataType>& left_operands,
                                                                   const ExpressionResult<RightOperandDataType>& right_operands,
                                                                   const Functor &functor);

 private:
  std::shared_ptr<const Chunk> _chunk;

  std::vector<std::unique_ptr<BaseColumnMaterialization>> _column_materializations;
};

}  // namespace opossum

#include "expression_evaluator.inl"