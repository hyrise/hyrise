#pragma once

#include <memory>
#include <vector>

#include "boost/variant.hpp"

#include "types.hpp"
#include "all_type_variant.hpp"
#include "null_value.hpp"
#include "logical_expression.hpp"

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
  template<typename T> using NullableValues = std::pair<std::vector<T>, std::vector<bool>>;
  template<typename T> using NonNullableValues = std::vector<T>;
  template<typename T> using NullableArrays = std::vector<NullableValues<T>>;
  template<typename T> using NonNullableArrays = std::vector<NonNullableValues<T>>;

  // Don't change the order! is_*() functions rely on which()
  template<typename T> using ExpressionResult = boost::variant<
    NullableValues<T>,
    NonNullableValues<T>,
    T,
    NullValue,
    NullableArrays<T>,
    NonNullableArrays<T>
  >;

  template<typename T> static bool is_nullable_values(const ExpressionResult<T>& result);
  template<typename T> static bool is_non_nullable_values(const ExpressionResult<T>& result);
  template<typename T> static bool is_value(const ExpressionResult<T>& result);
  template<typename T> static bool is_null(const ExpressionResult<T>& result);
  template<typename T> static bool is_nullable_array(const ExpressionResult<T>& result);
  template<typename T> static bool is_non_nullable_array(const ExpressionResult<T>& result);

  explicit ExpressionEvaluator(const std::shared_ptr<const Chunk>& chunk);

  void _ensure_column_materialization(const ColumnID column_id);

  std::shared_ptr<BaseColumn> evaluate_expression_to_column(const AbstractExpression& expression);

  template<typename T>
  ExpressionResult<T> evaluate_expression(const AbstractExpression& expression);

  template<typename T>
  ExpressionResult<T> evaluate_function_expression(const FunctionExpression& expression);

  template<typename OffsetDataType, typename CharCountDataType>
  ExpressionResult<std::string> evaluate_substring(const ExpressionResult<std::string>& string_result,
                                         const ExpressionResult<OffsetDataType>& offset_result,
                                         const ExpressionResult<CharCountDataType>& char_count_result);

  template<typename T>
  ExpressionResult<T> evaluate_arithmetic_expression(const ArithmeticExpression& expression);

  template<typename T>
  ExpressionResult<T> evaluate_logical_expression(const LogicalExpression& expression);

  template<typename T>
  ExpressionResult<T> evaluate_binary_predicate_expression(const BinaryPredicateExpression& expression);

  template<typename T>
  ExpressionResult<T> evaluate_in_expression(const InExpression& in_expression);

  template<typename T>
  ExpressionResult<T> evaluate_select_expression_for_chunk(const PQPSelectExpression &expression);

  template<typename T>
  ExpressionResult<T> evaluate_select_expression_for_row(const PQPSelectExpression& expression, const ChunkOffset chunk_offset);

  template<typename T>
  ExpressionResult<T> evaluate_case_expression(const CaseExpression& case_expression);

  template<typename T>
  ExpressionResult<T> evaluate_extract_expression(const ExtractExpression& extract_expression);

  template<size_t offset, size_t count>
  ExpressionResult<std::string> evaluate_extract_substr(const ExpressionResult<std::string>& from_result);

  template<typename T>
  ExpressionResult<T> evaluate_exists_expression(const ExistsExpression& exists_expression);

  template<typename ResultDataType,
          typename ThenDataType,
          typename ElseDataType>
  ExpressionResult<ResultDataType> evaluate_case(const ExpressionResult<int32_t>& when_result,
                                                 const ExpressionResult<ThenDataType>& then_result,
                                                 const ExpressionResult<ElseDataType>& else_result);

  template<typename T, template<typename...> typename Functor>
  ExpressionResult<T> evaluate_binary_expression(const AbstractExpression& left_operand,
                                                 const AbstractExpression& right_operand);

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
