#pragma once

#include <memory>
#include <vector>

#include <boost/variant.hpp>

#include "all_type_variant.hpp"
#include "expression/logical_expression.hpp"
#include "expression/pqp_subquery_expression.hpp"
#include "expression_result.hpp"
#include "null_value.hpp"
#include "types.hpp"

namespace hyrise {

class AbstractOperator;
class AbstractPredicateExpression;
class ArithmeticExpression;
class AbstractSegment;
class BinaryPredicateExpression;
class BetweenExpression;
class CaseExpression;
class CastExpression;
class Chunk;
class ExistsExpression;
class ExtractExpression;
class FunctionExpression;
class UnaryMinusExpression;
class InExpression;
class IsNullExpression;
class PQPColumnExpression;

/**
 * Computes the result of an Expression in three different ways
 *      - evaluate_expression_to_result(): result is a ExpressionResult<>, one entry per row in the input Chunk, or a
 *                                         single row if no input chunk is specified
 *      - evaluate_expression_to_segment(): wraps evaluate_expression_to_result() into a Segment.
 *      - evaluate_expression_to_pos_list(): Only for Expressions returning Bools; a PosList of the Rows where the
 *                                           Expression is True. Useful for, e.g., scans with complex predicates
 *
 * Operates either
 *      - ...on a Chunk, thus returning a value for each row in it
 *      - ...without a Chunk, thus returning a single value (and failing if Columns are encountered in the Expression)
 */
class ExpressionEvaluator final {
 public:
  // Hyrise doesn't have a bool column type, so we use int32_t. If at any point we get bool column types, just replace
  // all the occurences of Bool and DataTypeBool.
  using Bool = int32_t;
  static constexpr auto DataTypeBool = DataType::Int;

  // For Expressions that do not reference any columns (e.g. in the LIMIT clause)
  ExpressionEvaluator() = default;

  /*
   * For Expressions that reference segments from a single table
   * @param uncorrelated_subquery_results  Results from pre-computed uncorrelated selects, so they do not need to be
   *                                     evaluated for every chunk. Solely for performance.
   */
  ExpressionEvaluator(const std::shared_ptr<const Table>& table, const ChunkID chunk_id);

  std::shared_ptr<BaseValueSegment> evaluate_expression_to_segment(const AbstractExpression& expression);
  RowIDPosList evaluate_expression_to_pos_list(const AbstractExpression& expression);

  template <typename Result>
  std::shared_ptr<ExpressionResult<Result>> evaluate_expression_to_result(const AbstractExpression& expression);

 private:
  template <typename Result>
  std::shared_ptr<ExpressionResult<Result>> _evaluate_arithmetic_expression(const ArithmeticExpression& expression);

  template <typename Result>
  std::shared_ptr<ExpressionResult<Result>> _evaluate_logical_expression(const LogicalExpression& /*expression*/);

  template <typename Result>
  std::shared_ptr<ExpressionResult<Result>> _evaluate_predicate_expression(
      const AbstractPredicateExpression& /*predicate_expression*/);

  std::shared_ptr<ExpressionResult<Bool>> _evaluate_binary_predicate_expression(
      const BinaryPredicateExpression& expression);

  std::shared_ptr<ExpressionResult<Bool>> _evaluate_like_expression(const BinaryPredicateExpression& expression);

  std::shared_ptr<ExpressionResult<Bool>> _evaluate_is_null_expression(const IsNullExpression& expression);

  std::shared_ptr<ExpressionResult<Bool>> _evaluate_in_expression(const InExpression& in_expression);

  template <typename Result>
  std::shared_ptr<ExpressionResult<Result>> _evaluate_subquery_expression(
      const PQPSubqueryExpression& subquery_expression);

  std::vector<std::shared_ptr<const Table>> _evaluate_subquery_expression_to_tables(
      const PQPSubqueryExpression& expression);

  std::shared_ptr<const Table> _evaluate_subquery_expression_for_row(const PQPSubqueryExpression& expression,
                                                                     const ChunkOffset chunk_offset);

  template <typename Result>
  std::shared_ptr<ExpressionResult<Result>> _evaluate_column_expression(const PQPColumnExpression& column_expression);

  template <typename Result>
  std::shared_ptr<ExpressionResult<Result>> _evaluate_case_expression(const CaseExpression& case_expression);

  template <typename Result>
  std::shared_ptr<ExpressionResult<Result>> _evaluate_cast_expression(const CastExpression& cast_expression);

  template <typename Result>
  std::shared_ptr<ExpressionResult<Result>> _evaluate_value_or_correlated_parameter_expression(
      const AbstractExpression& expression);

  template <typename Result>
  std::shared_ptr<ExpressionResult<Result>> _evaluate_function_expression(const FunctionExpression& expression);

  template <typename Result>
  std::shared_ptr<ExpressionResult<Result>> _evaluate_extract_expression(const ExtractExpression& extract_expression);

  template <typename Result>
  std::shared_ptr<ExpressionResult<Result>> _evaluate_unary_minus_expression(
      const UnaryMinusExpression& unary_minus_expression);

  template <typename Result, typename Functor>
  std::shared_ptr<ExpressionResult<Result>> _evaluate_extract_component(const ExpressionResult<pmr_string>& from_result,
                                                                        const Functor extract_component);

  template <typename Result>
  std::shared_ptr<ExpressionResult<Result>> _evaluate_exists_expression(const ExistsExpression& /*exists_expression*/);

  template <typename Result>
  bool _evaluate_between_expression(const BetweenExpression& expression, Result& result_values);

  // See docs for `_evaluate_default_null_logic()`
  template <typename Result, typename Functor>
  std::shared_ptr<ExpressionResult<Result>> _evaluate_binary_with_default_null_logic(
      const AbstractExpression& left_expression, const AbstractExpression& right_expression);

  // The functor decides whether the result is null or not
  template <typename Result, typename Functor>
  std::shared_ptr<ExpressionResult<Result>> _evaluate_binary_with_functor_based_null_logic(
      const AbstractExpression& left_expression, const AbstractExpression& right_expression);

  template <typename Functor>
  void _resolve_to_expression_result_view(const AbstractExpression& expression, const Functor& functor);

  template <typename Functor>
  void _resolve_to_expression_result_views(const AbstractExpression& left_expression,
                                           const AbstractExpression& right_expression, const Functor& functor);

  template <typename Functor>
  void _resolve_to_expression_results(const AbstractExpression& left_expression,
                                      const AbstractExpression& right_expression, const Functor& functor);

  template <typename Functor>
  void _resolve_to_expression_result(const AbstractExpression& expression, const Functor& functor);

  /**
   * Compute the number of rows that any kind expression produces, given the number of rows in its parameters
   */
  template <typename... RowCounts>
  static ChunkOffset _result_size(const RowCounts... row_counts);

  /**
   * "Default null logic" means: if one of the arguments of an operation is NULL, the result is NULL
   *
   * Either operand can be either empty (the operand is not nullable), contain one element (the operand is a literal
   * with null info) or can have n rows (the operand is a nullable series)
   */
  static pmr_vector<bool> _evaluate_default_null_logic(const pmr_vector<bool>& left, const pmr_vector<bool>& right);

  void _materialize_segment_if_not_yet_materialized(const ColumnID column_id);

  std::shared_ptr<ExpressionResult<pmr_string>> _evaluate_substring(
      const std::vector<std::shared_ptr<AbstractExpression>>& arguments);
  std::shared_ptr<ExpressionResult<pmr_string>> _evaluate_concatenate(
      const std::vector<std::shared_ptr<AbstractExpression>>& arguments);
  template <typename Result>
  std::shared_ptr<ExpressionResult<Result>> _evaluate_absolute(const std::shared_ptr<AbstractExpression>& argument);

  template <typename Result>
  static std::vector<std::shared_ptr<ExpressionResult<Result>>> _prune_tables_to_expression_results(
      const std::vector<std::shared_ptr<const Table>>& tables);

  std::shared_ptr<const Table> _table;
  std::shared_ptr<const Chunk> _chunk;
  const ChunkID _chunk_id;
  size_t _output_row_count{1};

  // One entry for each segment in the _chunk, may be nullptr if the segment hasn't been materialized
  std::vector<std::shared_ptr<BaseExpressionResult>> _segment_materializations;

  // Some expressions can be reused, either in the same result column (SELECT (a+3)*(a+3)), or across columns
  // (TPC-H Q1)
  ConstExpressionUnorderedMap<std::shared_ptr<BaseExpressionResult>> _cached_expression_results;
};

}  // namespace hyrise
