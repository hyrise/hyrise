#pragma once

#include <memory>
#include <vector>

#include "boost/variant.hpp"

#include "types.hpp"
#include "all_type_variant.hpp"
#include "null_value.hpp"
#include "expression/logical_expression.hpp"
#include "expression_result.hpp"

namespace opossum {

class AbstractExpression;
class ArithmeticExpression;
class BaseColumn;
class BinaryPredicateExpression;
class CaseExpression;
class Chunk;
class ExistsExpression;
class ExtractExpression;
class FunctionExpression;
class InExpression;
class PQPSelectExpression;

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
  // For Expressions that do not reference any columns (e.g. in the LIMIT clause)
  ExpressionEvaluator() = default;

  // For Expressions that reference Columns from a single table
  explicit ExpressionEvaluator(const std::shared_ptr<const Chunk>& chunk);

  std::shared_ptr<BaseColumn> evaluate_expression_to_column(const AbstractExpression& expression);

  template<typename T>
  ExpressionResult<T> evaluate_expression(const AbstractExpression& expression);
//
//  template<typename T>
//  ExpressionResult<T> evaluate_function_expression(const FunctionExpression& expression);
//
//  template<typename OffsetDataType, typename CharCountDataType>
//  ExpressionResult<std::string> evaluate_substring(const ExpressionResult<std::string>& string_result,
//                                         const ExpressionResult<OffsetDataType>& offset_result,
//                                         const ExpressionResult<CharCountDataType>& char_count_result);
//
//  template<typename T>
//  ExpressionResult<T> evaluate_arithmetic_expression(const ArithmeticExpression& expression);
//
  template<typename T>
  ExpressionResult<T> evaluate_logical_expression(const LogicalExpression& expression);
//
//  template<typename T>
//  ExpressionResult<T> evaluate_binary_predicate_expression(const BinaryPredicateExpression& expression);

  template<typename T>
  ExpressionResult<T> evaluate_in_expression(const InExpression& in_expression);

//  template<typename T>
//  ExpressionResult<T> evaluate_array_expression(const ArrayExpression& array_expression);

//  template<typename T>
//  ExpressionResult<T> evaluate_select_expression_for_chunk(const PQPSelectExpression &expression);
//
//  template<typename T>
//  ExpressionResult<T> evaluate_select_expression_for_row(const PQPSelectExpression& expression, const ChunkOffset chunk_offset);
//
//  template<typename T>
//  ExpressionResult<T> evaluate_case_expression(const CaseExpression& case_expression);
//
//  template<typename T>
//  ExpressionResult<T> evaluate_extract_expression(const ExtractExpression& extract_expression);
//
//  template<size_t offset, size_t count>
//  ExpressionResult<std::string> evaluate_extract_substr(const ExpressionResult<std::string>& from_result);
//
//  template<typename T>
//  ExpressionResult<T> evaluate_exists_expression(const ExistsExpression& exists_expression);
//
//  template<typename ResultDataType,
//          typename ThenDataType,
//          typename ElseDataType>
//  ExpressionResult<ResultDataType> evaluate_case(const ExpressionResult<int32_t>& when_result,
//                                                 const ExpressionResult<ThenDataType>& then_result,
//                                                 const ExpressionResult<ElseDataType>& else_result);
//
//  template<typename T, template<typename...> typename Functor>
//  ExpressionResult<T> evaluate_binary_expression(const AbstractExpression& left_operand,
//                                                 const AbstractExpression& right_operand);
//
//
//
//  template<typename ResultDataType,
//           typename LeftOperandDataType,
//           typename RightOperandDataType,
//           typename Functor>
//  ExpressionResult<ResultDataType> evaluate_binary_operator(const ExpressionResult<LeftOperandDataType>& left_operands,
//                                                                   const ExpressionResult<RightOperandDataType>& right_operands,
//                                                                   const Functor &functor);


  template<typename R, typename Functor>
  ExpressionResult<R> evaluate_binary(const AbstractExpression& left_expression,
                                      const AbstractExpression& right_expression);

  template<typename Functor>
  void resolve_expression_to_iterator(const AbstractExpression& expression, const Functor& fn);

 private:
  std::shared_ptr<const Chunk> _chunk;
  size_t _output_row_count{0};

  std::vector<std::unique_ptr<BaseColumnMaterialization>> _column_materializations;
};

}  // namespace opossum
