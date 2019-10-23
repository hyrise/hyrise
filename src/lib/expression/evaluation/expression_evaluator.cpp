#include "expression_evaluator.hpp"

#include <iterator>
#include <type_traits>

#include "boost/lexical_cast.hpp"
#include "boost/variant/apply_visitor.hpp"

#include "all_parameter_variant.hpp"
#include "expression/abstract_expression.hpp"
#include "expression/abstract_predicate_expression.hpp"
#include "expression/arithmetic_expression.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/cast_expression.hpp"
#include "expression/exists_expression.hpp"
#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "expression/extract_expression.hpp"
#include "expression/function_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/list_expression.hpp"
#include "expression/logical_expression.hpp"
#include "expression/pqp_column_expression.hpp"
#include "expression/pqp_subquery_expression.hpp"
#include "expression/value_expression.hpp"
#include "expression_functors.hpp"
#include "hyrise.hpp"
#include "like_matcher.hpp"
#include "operators/abstract_operator.hpp"
#include "resolve_type.hpp"
#include "scheduler/operator_task.hpp"
#include "storage/segment_iterate.hpp"
#include "storage/value_segment.hpp"
#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"

using namespace std::string_literals;            // NOLINT
using namespace opossum::expression_functional;  // NOLINT

namespace {

using namespace opossum;  // NOLINT

template <typename Functor>
void resolve_binary_predicate_evaluator(const PredicateCondition predicate_condition, const Functor functor) {
  /**
   * Instantiate @param functor for each PredicateCondition
   */

  // clang-format off
  switch (predicate_condition) {
    case PredicateCondition::Equals:            functor(boost::hana::type<EqualsEvaluator>{});            break;
    case PredicateCondition::NotEquals:         functor(boost::hana::type<NotEqualsEvaluator>{});         break;
    case PredicateCondition::LessThan:          functor(boost::hana::type<LessThanEvaluator >{});         break;
    case PredicateCondition::LessThanEquals:    functor(boost::hana::type<LessThanEqualsEvaluator>{});    break;
    case PredicateCondition::GreaterThan:
    case PredicateCondition::GreaterThanEquals:
      Fail("PredicateCondition should have been flipped");
      break;

    default:
      Fail("PredicateCondition should be handled in different function");
  }
  // clang-format on
}

std::shared_ptr<AbstractExpression> rewrite_between_expression(const AbstractExpression& expression) {
  // `a BETWEEN b AND c` --> `a >= b AND a <= c`
  //
  // (This is desirable because three expression data types (from three arguments) generate many type
  // combinations and thus lengthen compile time and increase binary size notably.)

  const auto* between_expression = dynamic_cast<const BetweenExpression*>(&expression);
  Assert(between_expression, "Expected Between Expression");

  const auto lower_expression =
      is_lower_inclusive_between(between_expression->predicate_condition)
          ? greater_than_equals_(between_expression->value(), between_expression->lower_bound())
          : greater_than_(between_expression->value(), between_expression->lower_bound());

  const auto upper_expression = is_upper_inclusive_between(between_expression->predicate_condition)
                                    ? less_than_equals_(between_expression->value(), between_expression->upper_bound())
                                    : less_than_(between_expression->value(), between_expression->upper_bound());

  return and_(lower_expression, upper_expression);
}

std::shared_ptr<AbstractExpression> rewrite_in_list_expression(const InExpression& in_expression) {
  /**
   * "a IN (x, y, z)"   ---->   "a = x OR a = y OR a = z"
   * "a NOT IN (x, y, z)"   ---->   "a != x AND a != y AND a != z"
   *
   * Out of array_expression.elements(), pick those expressions whose type can be compared with
   * in_expression.value() so we're not getting "Can't compare Int and String" when doing something crazy like
   * "5 IN (6, 5, "Hello")
   */

  const auto list_expression = std::dynamic_pointer_cast<ListExpression>(in_expression.set());
  Assert(list_expression, "Expected ListExpression");

  const auto left_is_string = in_expression.value()->data_type() == DataType::String;
  std::vector<std::shared_ptr<AbstractExpression>> type_compatible_elements;
  for (const auto& element : list_expression->elements()) {
    if ((element->data_type() == DataType::String) == left_is_string) {
      type_compatible_elements.emplace_back(element);
    }
  }

  if (type_compatible_elements.empty()) {
    // `5 IN ()` is FALSE as is `NULL IN ()`
    return value_(0);
  }

  std::shared_ptr<AbstractExpression> rewritten_expression;

  if (in_expression.is_negated()) {
    // a NOT IN (1,2,3) --> a != 1 AND a != 2 AND a != 3
    rewritten_expression = not_equals_(in_expression.value(), type_compatible_elements.front());
    for (auto element_idx = size_t{1}; element_idx < type_compatible_elements.size(); ++element_idx) {
      const auto equals_element = not_equals_(in_expression.value(), type_compatible_elements[element_idx]);
      rewritten_expression = and_(rewritten_expression, equals_element);
    }
  } else {
    // a IN (1,2,3) --> a == 1 OR a == 2 OR a == 3
    rewritten_expression = equals_(in_expression.value(), type_compatible_elements.front());
    for (auto element_idx = size_t{1}; element_idx < type_compatible_elements.size(); ++element_idx) {
      const auto equals_element = equals_(in_expression.value(), type_compatible_elements[element_idx]);
      rewritten_expression = or_(rewritten_expression, equals_element);
    }
  }

  return rewritten_expression;
}

}  // namespace

namespace opossum {

ExpressionEvaluator::ExpressionEvaluator(
    const std::shared_ptr<const Table>& table, const ChunkID chunk_id,
    const std::shared_ptr<const UncorrelatedSubqueryResults>& uncorrelated_subquery_results)
    : _table(table),
      _chunk(_table->get_chunk(chunk_id)),
      _chunk_id(chunk_id),
      _uncorrelated_subquery_results(uncorrelated_subquery_results) {
  _output_row_count = _chunk->size();
  _segment_materializations.resize(_chunk->column_count());
}

template <typename Result>
std::shared_ptr<ExpressionResult<Result>> ExpressionEvaluator::evaluate_expression_to_result(
    const AbstractExpression& expression) {
  // First, look in the cache
  const auto expression_ptr = expression.shared_from_this();
  const auto cached_result_iter = _cached_expression_results.find(expression_ptr);
  if (cached_result_iter != _cached_expression_results.end()) {
    return std::static_pointer_cast<ExpressionResult<Result>>(cached_result_iter->second);
  }

  // Ok, we have to actually work...
  auto result = std::shared_ptr<ExpressionResult<Result>>{};

  switch (expression.type) {
    case ExpressionType::Arithmetic:
      result = _evaluate_arithmetic_expression<Result>(static_cast<const ArithmeticExpression&>(expression));
      break;

    case ExpressionType::Logical:
      result = _evaluate_logical_expression<Result>(static_cast<const LogicalExpression&>(expression));
      break;

    case ExpressionType::Predicate:
      result = _evaluate_predicate_expression<Result>(static_cast<const AbstractPredicateExpression&>(expression));
      break;

    case ExpressionType::PQPSubquery:
      result = _evaluate_subquery_expression<Result>(*static_cast<const PQPSubqueryExpression*>(&expression));
      break;

    case ExpressionType::PQPColumn:
      result = _evaluate_column_expression<Result>(*static_cast<const PQPColumnExpression*>(&expression));
      break;

    // ValueExpression and CorrelatedParameterExpression both need to unpack an AllTypeVariant, so one functions handles
    // both
    case ExpressionType::CorrelatedParameter:
    case ExpressionType::Value:
      result = _evaluate_value_or_correlated_parameter_expression<Result>(expression);
      break;

    case ExpressionType::Function:
      result = _evaluate_function_expression<Result>(static_cast<const FunctionExpression&>(expression));
      break;

    case ExpressionType::Case:
      result = _evaluate_case_expression<Result>(static_cast<const CaseExpression&>(expression));
      break;

    case ExpressionType::Cast:
      result = _evaluate_cast_expression<Result>(static_cast<const CastExpression&>(expression));
      break;

    case ExpressionType::Exists:
      result = _evaluate_exists_expression<Result>(static_cast<const ExistsExpression&>(expression));
      break;

    case ExpressionType::Extract:
      result = _evaluate_extract_expression<Result>(static_cast<const ExtractExpression&>(expression));
      break;

    case ExpressionType::UnaryMinus:
      result = _evaluate_unary_minus_expression<Result>(static_cast<const UnaryMinusExpression&>(expression));
      break;

    case ExpressionType::Aggregate:
      Fail("ExpressionEvaluator doesn't support Aggregates, use the Aggregate Operator to compute them");

    case ExpressionType::List:
      Fail("Can't evaluate a ListExpression, lists should only appear as the right operand of an InExpression");

    case ExpressionType::LQPColumn:
    case ExpressionType::LQPSubquery:
      Fail("Can't evaluate an LQP expression, those need to be translated by the LQPTranslator first.");

    case ExpressionType::Placeholder:
      Fail(
          "Can't evaluate an expressions still containing placeholders. Are you trying to execute a PreparedPlan "
          "without instantiating it first?");
  }

  // Store the result in the cache
  _cached_expression_results.insert(cached_result_iter, {expression_ptr, result});

  return std::static_pointer_cast<ExpressionResult<Result>>(result);
}

template <typename Result>
std::shared_ptr<ExpressionResult<Result>> ExpressionEvaluator::_evaluate_arithmetic_expression(
    const ArithmeticExpression& expression) {
  const auto& left = *expression.left_operand();
  const auto& right = *expression.right_operand();

  // clang-format off
  switch (expression.arithmetic_operator) {
    case ArithmeticOperator::Addition:       return _evaluate_binary_with_default_null_logic<Result, AdditionEvaluator>(left, right);  // NOLINT
    case ArithmeticOperator::Subtraction:    return _evaluate_binary_with_default_null_logic<Result, SubtractionEvaluator>(left, right);  // NOLINT
    case ArithmeticOperator::Multiplication: return _evaluate_binary_with_default_null_logic<Result, MultiplicationEvaluator>(left, right);  // NOLINT

    // Division and Modulo need to catch division by zero
    case ArithmeticOperator::Division:       return _evaluate_binary_with_functor_based_null_logic<Result, DivisionEvaluator>(left, right);  // NOLINT
    case ArithmeticOperator::Modulo:         return _evaluate_binary_with_functor_based_null_logic<Result, ModuloEvaluator>(left, right);  // NOLINT
  }
  // clang-format on
  Fail("Invalid enum value");
}

template <>
std::shared_ptr<ExpressionResult<ExpressionEvaluator::Bool>>
ExpressionEvaluator::_evaluate_binary_predicate_expression<ExpressionEvaluator::Bool>(
    const BinaryPredicateExpression& expression) {
  auto result = std::shared_ptr<ExpressionResult<ExpressionEvaluator::Bool>>{};

  // To reduce the number of template instantiations, we flip > and >= to < and <=
  auto predicate_condition = expression.predicate_condition;
  const bool flip = predicate_condition == PredicateCondition::GreaterThan ||
                    predicate_condition == PredicateCondition::GreaterThanEquals;
  if (flip) predicate_condition = flip_predicate_condition(predicate_condition);
  const auto& left = flip ? *expression.right_operand() : *expression.left_operand();
  const auto& right = flip ? *expression.left_operand() : *expression.right_operand();

  // clang-format off
  resolve_binary_predicate_evaluator(predicate_condition, [&](const auto evaluator_t) {
    using Evaluator = typename decltype(evaluator_t)::type;
    result = _evaluate_binary_with_default_null_logic<ExpressionEvaluator::Bool, Evaluator>(left, right);  // NOLINT
  });

  return result;
  // clang-format on
}

template <typename Result>
std::shared_ptr<ExpressionResult<Result>> ExpressionEvaluator::_evaluate_binary_predicate_expression(
    const BinaryPredicateExpression& expression) {
  Fail("Can only evaluate predicates to bool");
}

template <>
std::shared_ptr<ExpressionResult<ExpressionEvaluator::Bool>>
ExpressionEvaluator::_evaluate_like_expression<ExpressionEvaluator::Bool>(const BinaryPredicateExpression& expression) {
  /**
   * NOTE: This code path is NOT taken for LIKEs in predicates. That is `SELECT * FROM t WHERE a LIKE '%Hello%'` is
   *        handled in the TableScan. This code path is for `SELECT a LIKE 'bla' FROM ...` and alike
   */

  Assert(expression.predicate_condition == PredicateCondition::Like ||
             expression.predicate_condition == PredicateCondition::NotLike,
         "Expected PredicateCondition Like or NotLike");

  const auto left_results = evaluate_expression_to_result<pmr_string>(*expression.left_operand());
  const auto right_results = evaluate_expression_to_result<pmr_string>(*expression.right_operand());

  const auto invert_results = expression.predicate_condition == PredicateCondition::NotLike;

  const auto result_size = _result_size(left_results->size(), right_results->size());
  auto result_values = std::vector<ExpressionEvaluator::Bool>(result_size, 0);

  /**
   * Three different kinds of LIKE are considered for performance reasons and avoid redundant creation of the
   * LikeMatcher
   *    - `a LIKE b`
   *    - `a LIKE '%hello%'`
   *    - `'hello' LIKE b`
   */
  const auto both_are_literals = left_results->is_literal() && right_results->is_literal();
  const auto both_are_series = !left_results->is_literal() && !right_results->is_literal();
  if (both_are_literals || both_are_series) {
    // E.g., `a LIKE b` - A new matcher for each row and a different value as well
    for (auto row_idx = ChunkOffset{0}; row_idx < result_size; ++row_idx) {
      LikeMatcher{right_results->values[row_idx]}.resolve(invert_results, [&](const auto& matcher) {
        result_values[row_idx] = matcher(left_results->values[row_idx]);
      });
    }
  } else if (!left_results->is_literal() && right_results->is_literal()) {
    // E.g., `a LIKE '%hello%'` -- A single matcher for all rows
    LikeMatcher like_matcher{right_results->values.front()};

    for (auto row_idx = ChunkOffset{0}; row_idx < result_size; ++row_idx) {
      like_matcher.resolve(invert_results, [&](const auto& matcher) {
        result_values[row_idx] = matcher(left_results->values[row_idx]);
      });
    }
  } else {
    // E.g., `'hello' LIKE b` -- A new matcher for each row but the value to check is constant
    for (auto row_idx = ChunkOffset{0}; row_idx < result_size; ++row_idx) {
      LikeMatcher{right_results->values[row_idx]}.resolve(
          invert_results, [&](const auto& matcher) { result_values[row_idx] = matcher(left_results->values.front()); });
    }
  }

  auto result_nulls = _evaluate_default_null_logic(left_results->nulls, right_results->nulls);

  return std::make_shared<ExpressionResult<ExpressionEvaluator::Bool>>(std::move(result_values),
                                                                       std::move(result_nulls));
}

template <typename Result>
std::shared_ptr<ExpressionResult<Result>> ExpressionEvaluator::_evaluate_like_expression(
    const BinaryPredicateExpression& expression) {
  Fail("Can only evaluate predicates to bool");
}

template <>
std::shared_ptr<ExpressionResult<ExpressionEvaluator::Bool>>
ExpressionEvaluator::_evaluate_is_null_expression<ExpressionEvaluator::Bool>(const IsNullExpression& expression) {
  std::vector<ExpressionEvaluator::Bool> result_values;

  _resolve_to_expression_result_view(*expression.operand(), [&](const auto& view) {
    result_values.resize(view.size());

    if (expression.predicate_condition == PredicateCondition::IsNull) {
      for (auto chunk_offset = ChunkOffset{0}; chunk_offset < static_cast<ChunkOffset>(view.size()); ++chunk_offset) {
        result_values[chunk_offset] = view.is_null(chunk_offset);
      }
    } else {  // PredicateCondition::IsNotNull
      for (auto chunk_offset = ChunkOffset{0}; chunk_offset < static_cast<ChunkOffset>(view.size()); ++chunk_offset) {
        result_values[chunk_offset] = !view.is_null(chunk_offset);
      }
    }
  });

  return std::make_shared<ExpressionResult<ExpressionEvaluator::Bool>>(std::move(result_values));
}

template <typename Result>
std::shared_ptr<ExpressionResult<Result>> ExpressionEvaluator::_evaluate_is_null_expression(
    const IsNullExpression& expression) {
  Fail("Can only evaluate predicates to bool");
}

template <>
std::shared_ptr<ExpressionResult<ExpressionEvaluator::Bool>>
ExpressionEvaluator::_evaluate_in_expression<ExpressionEvaluator::Bool>(const InExpression& in_expression) {
  const auto& left_expression = *in_expression.value();
  const auto& right_expression = *in_expression.set();

  std::vector<ExpressionEvaluator::Bool> result_values;
  std::vector<bool> result_nulls;

  if (right_expression.type == ExpressionType::List) {
    const auto& list_expression = static_cast<const ListExpression&>(right_expression);

    if (list_expression.elements().empty()) {
      // `x IN ()` is false/`x NOT IN ()` is true, even if this is not supported by SQL
      return std::make_shared<ExpressionResult<ExpressionEvaluator::Bool>>(
          std::vector<ExpressionEvaluator::Bool>{in_expression.is_negated()});
    }

    if (left_expression.data_type() == DataType::Null) {
      // `NULL [NOT] IN ...` is NULL
      return std::make_shared<ExpressionResult<ExpressionEvaluator::Bool>>(std::vector<ExpressionEvaluator::Bool>{0},
                                                                           std::vector<bool>{true});
    }

    /**
     * Out of array_expression.elements(), pick those expressions whose type can be compared with
     * in_expression.value() so we're not getting "Can't compare Int and String" when doing something crazy like
     * "5 IN (6, 5, "Hello")
     */
    const auto left_is_string = left_expression.data_type() == DataType::String;
    std::vector<std::shared_ptr<AbstractExpression>> type_compatible_elements;
    bool all_elements_are_values_of_left_type = true;
    resolve_data_type(left_expression.data_type(), [&](const auto left_data_type_t) {
      using LeftDataType = typename decltype(left_data_type_t)::type;

      for (const auto& element : list_expression.elements()) {
        if ((element->data_type() == DataType::String) == left_is_string) {
          type_compatible_elements.emplace_back(element);
        }

        if (element->type != ExpressionType::Value) {
          all_elements_are_values_of_left_type = false;
        } else {
          const auto& value_expression = std::static_pointer_cast<ValueExpression>(element);
          if (value_expression->value.type() != typeid(LeftDataType)) all_elements_are_values_of_left_type = false;
        }
      }
    });

    if (type_compatible_elements.empty()) {
      // `x IN ()` is false/`x NOT IN ()` is true, even if this is not supported by SQL
      return std::make_shared<ExpressionResult<ExpressionEvaluator::Bool>>(
          std::vector<ExpressionEvaluator::Bool>{in_expression.is_negated()});
    }

    // If all elements of the list are simple values (e.g., `IN (1, 2, 3)`), iterate over the column and directly
    // compare the left value with the values in the list.
    //
    // If we can't store the values in a vector (because they are of non-literals or of different types), we translate
    // the IN clause to a series of ORs:
    // "a IN (x, y, z)"   ---->   "a = x OR a = y OR a = z"
    // The first path is faster, while the second one is more flexible.
    if (all_elements_are_values_of_left_type) {
      _resolve_to_expression_result_view(left_expression, [&](const auto& left_view) {
        using LeftDataType = typename std::decay_t<decltype(left_view)>::Type;

        // Above, we have ruled out NULL on the left side, but the compiler does not know this yet
        if constexpr (!std::is_same_v<LeftDataType, NullValue>) {
          std::vector<LeftDataType> right_values(type_compatible_elements.size());
          auto right_values_idx = size_t{0};
          for (const auto& expression : type_compatible_elements) {
            const auto& value_expression = std::static_pointer_cast<ValueExpression>(expression);
            right_values[right_values_idx] = boost::get<LeftDataType>(value_expression->value);
            right_values_idx++;
          }

          result_values.resize(left_view.size(), in_expression.is_negated());
          if (left_view.is_nullable()) {
            result_nulls.resize(left_view.size());
          }

          for (auto chunk_offset = ChunkOffset{0}; chunk_offset < static_cast<ChunkOffset>(left_view.size());
               ++chunk_offset) {
            if (left_view.is_nullable() && left_view.is_null(chunk_offset)) {
              result_nulls[chunk_offset] = true;
              continue;
            }
            // We could sort right_values and perform a binary search. However, a linear search is better suited for
            // small vectors. For bigger IN lists, the InExpressionRewriteRule will switch to a hash join, anyway.
            if (auto it = std::find(right_values.cbegin(), right_values.cend(), left_view.value(chunk_offset));
                it != right_values.cend() && *it == left_view.value(chunk_offset)) {
              result_values[chunk_offset] = !in_expression.is_negated();
            }
          }
        } else {
          Fail("Should have ruled out NullValues on the left side of IN by now");
        }
      });

      return std::make_shared<ExpressionResult<ExpressionEvaluator::Bool>>(std::move(result_values),
                                                                           std::move(result_nulls));
    }
    PerformanceWarning("Using slow path for IN expression");

    // Nope, it is a list with diverse types - falling back to rewrite of expression:
    return evaluate_expression_to_result<ExpressionEvaluator::Bool>(*rewrite_in_list_expression(in_expression));

  } else if (right_expression.type == ExpressionType::PQPSubquery) {
    const auto* subquery_expression = dynamic_cast<const PQPSubqueryExpression*>(&right_expression);
    Assert(subquery_expression, "Expected PQPSubqueryExpression");

    const auto subquery_result_tables = _evaluate_subquery_expression_to_tables(*subquery_expression);

    resolve_data_type(subquery_expression->data_type(), [&](const auto subquery_data_type_t) {
      using SubqueryDataType = typename decltype(subquery_data_type_t)::type;

      const auto subquery_results = _prune_tables_to_expression_results<SubqueryDataType>(subquery_result_tables);
      Assert(subquery_results.size() == 1 || subquery_results.size() == _output_row_count,
             "Unexpected number of lists returned from Subquery. "
             "Should be one (if the Subquery is not correlated), or one per row (if it is)");

      _resolve_to_expression_result_view(left_expression, [&](const auto& left_view) {
        using ValueDataType = typename std::decay_t<decltype(left_view)>::Type;

        if constexpr (EqualsEvaluator::supports_v<ExpressionEvaluator::Bool, ValueDataType, SubqueryDataType>) {
          const auto result_size = _result_size(left_view.size(), subquery_results.size());

          result_values.resize(result_size);
          // TODO(moritz) The InExpression doesn't in all cases need to return a nullable
          result_nulls.resize(result_size);

          for (auto chunk_offset = ChunkOffset{0}; chunk_offset < static_cast<ChunkOffset>(result_size);
               ++chunk_offset) {
            // If the SELECT returned just one list, always perform the IN check with that one list
            // If the SELECT returned multiple lists, then the Subquery was correlated and we need to do the IN check
            // against the list of the current row
            const auto& list = *subquery_results[subquery_results.size() == 1 ? 0 : chunk_offset];

            auto list_contains_null = false;

            for (auto list_element_idx = ChunkOffset{0}; list_element_idx < static_cast<ChunkOffset>(list.size());
                 ++list_element_idx) {
              // `a IN (x,y,z)` is supposed to have the same semantics as `a = x OR a = y OR a = z`, so we use `Equals`
              // here as well.
              EqualsEvaluator{}(result_values[chunk_offset], list.value(list_element_idx),
                                left_view.value(chunk_offset));
              if (result_values[chunk_offset]) break;

              list_contains_null |= list.is_null(list_element_idx);
            }

            result_nulls[chunk_offset] =
                (result_values[chunk_offset] == 0 && list_contains_null) || left_view.is_null(chunk_offset);

            if (in_expression.is_negated()) result_values[chunk_offset] = result_values[chunk_offset] == 0 ? 1 : 0;
          }

        } else {
          // Tried to do, e.g., `5 IN (<subquery_returning_string>)` - return bool instead of failing, because that's
          // what we do for `5 IN ('Hello', 'World')
          result_values.resize(1, in_expression.is_negated() ? 1 : 0);
        }
      });
    });

  } else {
    /**
     * `<expression> IN <anything_but_list_or_subquery>` is not legal SQL, but on expression level we have to support
     * it, since `<anything_but_list_or_subquery>` might be a column holding the result of a subquery.
     * To accomplish this, we simply rewrite the expression to `<expression> IN LIST(<anything_but_list_or_subquery>)`.
     */

    return _evaluate_in_expression<ExpressionEvaluator::Bool>(*std::make_shared<InExpression>(
        in_expression.predicate_condition, in_expression.value(), list_(in_expression.set())));
  }

  return std::make_shared<ExpressionResult<ExpressionEvaluator::Bool>>(std::move(result_values),
                                                                       std::move(result_nulls));
}

template <typename Result>
std::shared_ptr<ExpressionResult<Result>> ExpressionEvaluator::_evaluate_in_expression(
    const InExpression& in_expression) {
  Fail("InExpression supports only bool as result");
}

template <>
std::shared_ptr<ExpressionResult<ExpressionEvaluator::Bool>>
ExpressionEvaluator::_evaluate_predicate_expression<ExpressionEvaluator::Bool>(
    const AbstractPredicateExpression& predicate_expression) {
  /**
   * NOTE: This evaluates predicates, but typical predicates in the WHERE clause of an SQL query will not take this
   * path and go through a dedicates scan operator (e.g. TableScan)
   */

  switch (predicate_expression.predicate_condition) {
    case PredicateCondition::Equals:
    case PredicateCondition::LessThanEquals:
    case PredicateCondition::GreaterThanEquals:
    case PredicateCondition::GreaterThan:
    case PredicateCondition::NotEquals:
    case PredicateCondition::LessThan:
      return _evaluate_binary_predicate_expression<ExpressionEvaluator::Bool>(
          static_cast<const BinaryPredicateExpression&>(predicate_expression));

    case PredicateCondition::BetweenInclusive:
    case PredicateCondition::BetweenLowerExclusive:
    case PredicateCondition::BetweenUpperExclusive:
    case PredicateCondition::BetweenExclusive:
      return evaluate_expression_to_result<ExpressionEvaluator::Bool>(
          *rewrite_between_expression(predicate_expression));

    case PredicateCondition::In:
    case PredicateCondition::NotIn:
      return _evaluate_in_expression<ExpressionEvaluator::Bool>(static_cast<const InExpression&>(predicate_expression));

    case PredicateCondition::Like:
    case PredicateCondition::NotLike:
      return _evaluate_like_expression<ExpressionEvaluator::Bool>(
          static_cast<const BinaryPredicateExpression&>(predicate_expression));

    case PredicateCondition::IsNull:
    case PredicateCondition::IsNotNull:
      return _evaluate_is_null_expression<ExpressionEvaluator::Bool>(
          static_cast<const IsNullExpression&>(predicate_expression));
  }
  Fail("Invalid enum value");
}

template <typename Result>
std::shared_ptr<ExpressionResult<Result>> ExpressionEvaluator::_evaluate_predicate_expression(
    const AbstractPredicateExpression& predicate_expression) {
  Fail("Can only evaluate predicates to bool");
}

template <typename Result>
std::shared_ptr<ExpressionResult<Result>> ExpressionEvaluator::_evaluate_column_expression(
    const PQPColumnExpression& column_expression) {
  Assert(_chunk, "Cannot access columns in this Expression as it doesn't operate on a Table/Chunk");

  const auto& segment = *_chunk->get_segment(column_expression.column_id);
  Assert(segment.data_type() == data_type_from_type<Result>(), "Can't evaluate segment to different type");

  _materialize_segment_if_not_yet_materialized(column_expression.column_id);
  return std::static_pointer_cast<ExpressionResult<Result>>(_segment_materializations[column_expression.column_id]);
}

template <typename Result>
std::shared_ptr<ExpressionResult<Result>> ExpressionEvaluator::_evaluate_case_expression(
    const CaseExpression& case_expression) {
  const auto when = evaluate_expression_to_result<ExpressionEvaluator::Bool>(*case_expression.when());

  std::vector<Result> values;
  std::vector<bool> nulls;

  _resolve_to_expression_results(
      *case_expression.then(), *case_expression.otherwise(), [&](const auto& then_result, const auto& else_result) {
        using ThenResultType = typename std::decay_t<decltype(then_result)>::Type;
        using ElseResultType = typename std::decay_t<decltype(else_result)>::Type;

        if constexpr (CaseEvaluator::supports_v<Result, ThenResultType, ElseResultType>) {
          const auto result_size = _result_size(when->size(), then_result.size(), else_result.size());
          values.resize(result_size);
          nulls.resize(result_size);

          for (auto chunk_offset = ChunkOffset{0}; chunk_offset < static_cast<ChunkOffset>(result_size);
               ++chunk_offset) {
            if (when->value(chunk_offset) && !when->is_null(chunk_offset)) {
              values[chunk_offset] = to_value<Result>(then_result.value(chunk_offset));
              nulls[chunk_offset] = then_result.is_null(chunk_offset);
            } else {
              values[chunk_offset] = to_value<Result>(else_result.value(chunk_offset));
              nulls[chunk_offset] = else_result.is_null(chunk_offset);
            }
          }
        } else {
          Fail("Illegal operands for CaseExpression");
        }
      });

  return std::make_shared<ExpressionResult<Result>>(std::move(values), std::move(nulls));
}

template <typename Result>
std::shared_ptr<ExpressionResult<Result>> ExpressionEvaluator::_evaluate_cast_expression(
    const CastExpression& cast_expression) {
  /**
   * Implements SQL's CAST with the following semantics
   *    Float/Double -> Int/Long:           Value gets floor()ed
   *    String -> Int/Long/Float/Double:    Conversion is attempted, on error zero is returned
   *                                        in accordance with SQLite. (" 5hallo" AS INT) -> 5
   *    NULL -> Any type                    A nulled value of the requested type is returned.
   */

  auto values = std::vector<Result>{};
  auto nulls = std::vector<bool>{};

  _resolve_to_expression_result(*cast_expression.argument(), [&](const auto& argument_result) {
    using ArgumentDataType = typename std::decay_t<decltype(argument_result)>::Type;

    const auto result_size = _result_size(argument_result.size());

    values.resize(result_size);

    for (auto chunk_offset = ChunkOffset{0}; chunk_offset < static_cast<ChunkOffset>(result_size); ++chunk_offset) {
      const auto& argument_value = argument_result.value(chunk_offset);

      // NOLINTNEXTLINE(bugprone-branch-clone)
      if constexpr (std::is_same_v<Result, NullValue> || std::is_same_v<ArgumentDataType, NullValue>) {
        // "<Something> to Null" cast. Do nothing, this is handled by the `nulls` vector
      } else if constexpr (std::is_same_v<Result, pmr_string>) {  // NOLINT
        // "<Something> to String" cast. Sould never fail, thus boost::lexical_cast (which throws on error) is fine
        values[chunk_offset] = boost::lexical_cast<Result>(argument_value);
      } else {
        if constexpr (std::is_same_v<ArgumentDataType, pmr_string>) {  // NOLINT
          // "String to Numeric" cast
          // Same as in SQLite, an illegal conversion (e.g. CAST("Hello" AS INT)) yields zero
          // Does NOT use boost::lexical_cast() as that would throw on error - and we do not do the
          // exception-as-flow-control thing.
          if (!boost::conversion::try_lexical_convert(argument_value, values[chunk_offset])) {
            values[chunk_offset] = 0;
          }
        } else {
          // "Numeric to Numeric" cast. Use static_cast<> as boost::conversion::try_lexical_convert() would fail for
          // CAST(5.5 AS INT)
          values[chunk_offset] = static_cast<Result>(argument_value);
        }
      }
    }

    nulls = argument_result.nulls;
  });

  return std::make_shared<ExpressionResult<Result>>(std::move(values), std::move(nulls));
}

template <>
std::shared_ptr<ExpressionResult<ExpressionEvaluator::Bool>>
ExpressionEvaluator::_evaluate_exists_expression<ExpressionEvaluator::Bool>(const ExistsExpression& exists_expression) {
  const auto subquery_expression = std::dynamic_pointer_cast<PQPSubqueryExpression>(exists_expression.subquery());
  Assert(subquery_expression, "Expected PQPSubqueryExpression");

  const auto subquery_result_tables = _evaluate_subquery_expression_to_tables(*subquery_expression);

  std::vector<ExpressionEvaluator::Bool> result_values(subquery_result_tables.size());

  switch (exists_expression.exists_expression_type) {
    case ExistsExpressionType::Exists:
      for (auto chunk_offset = ChunkOffset{0}; chunk_offset < static_cast<ChunkOffset>(subquery_result_tables.size());
           ++chunk_offset) {
        result_values[chunk_offset] = subquery_result_tables[chunk_offset]->row_count() > 0;
      }
      break;

    case ExistsExpressionType::NotExists:
      for (auto chunk_offset = ChunkOffset{0}; chunk_offset < static_cast<ChunkOffset>(subquery_result_tables.size());
           ++chunk_offset) {
        result_values[chunk_offset] = subquery_result_tables[chunk_offset]->row_count() == 0;
      }
      break;
  }

  return std::make_shared<ExpressionResult<ExpressionEvaluator::Bool>>(std::move(result_values));
}

template <typename Result>
std::shared_ptr<ExpressionResult<Result>> ExpressionEvaluator::_evaluate_exists_expression(
    const ExistsExpression& exists_expression) {
  Fail("Exists can only return bool");
}

template <typename Result>
std::shared_ptr<ExpressionResult<Result>> ExpressionEvaluator::_evaluate_value_or_correlated_parameter_expression(
    const AbstractExpression& expression) {
  AllTypeVariant value;

  if (expression.type == ExpressionType::Value) {
    const auto& value_expression = static_cast<const ValueExpression&>(expression);
    value = value_expression.value;
  } else {
    const auto& correlated_parameter_expression = dynamic_cast<const CorrelatedParameterExpression*>(&expression);
    Assert(correlated_parameter_expression, "ParameterExpression not a CorrelatedParameterExpression");
    Assert(correlated_parameter_expression->value(), "CorrelatedParameterExpression: Value not set, cannot evaluate");
    value = *correlated_parameter_expression->value();
  }

  if (value.type() == typeid(NullValue)) {
    // NullValue can be evaluated to any type - it is then a null value of that type.
    // This makes it easier to implement expressions where a certain data type is expected, but a Null literal is
    // given. Think `CASE NULL THEN ... ELSE ...` - the NULL will be evaluated to be a bool.
    std::vector<bool> nulls{};
    nulls.emplace_back(true);
    return std::make_shared<ExpressionResult<Result>>(std::vector<Result>{{Result{}}}, nulls);
  } else {
    Assert(value.type() == typeid(Result), "Can't evaluate ValueExpression to requested type Result");
    return std::make_shared<ExpressionResult<Result>>(std::vector<Result>{{boost::get<Result>(value)}});
  }
}

template <typename Result>
std::shared_ptr<ExpressionResult<Result>> ExpressionEvaluator::_evaluate_function_expression(
    const FunctionExpression& expression) {
  switch (expression.function_type) {
    case FunctionType::Concatenate:
    case FunctionType::Substring:
      if constexpr (std::is_same_v<Result, pmr_string>) {
        switch (expression.function_type) {
          case FunctionType::Substring:
            return _evaluate_substring(expression.arguments);
          case FunctionType::Concatenate:
            return _evaluate_concatenate(expression.arguments);
        }
      } else {
        Fail("Function can only be evaluated to a string");
      }
  }
  Fail("Invalid enum value");
}

template <>
std::shared_ptr<ExpressionResult<pmr_string>> ExpressionEvaluator::_evaluate_extract_expression<pmr_string>(
    const ExtractExpression& extract_expression) {
  const auto from_result = evaluate_expression_to_result<pmr_string>(*extract_expression.from());

  switch (extract_expression.datetime_component) {
    case DatetimeComponent::Year:
      return _evaluate_extract_substr<0, 4>(*from_result);
    case DatetimeComponent::Month:
      return _evaluate_extract_substr<5, 2>(*from_result);
    case DatetimeComponent::Day:
      return _evaluate_extract_substr<8, 2>(*from_result);

    case DatetimeComponent::Hour:
    case DatetimeComponent::Minute:
    case DatetimeComponent::Second:
      Fail("Hour, Minute and Second not available in String Datetimes");
  }
  Fail("Invalid enum value");
}

template <typename Result>
std::shared_ptr<ExpressionResult<Result>> ExpressionEvaluator::_evaluate_extract_expression(
    const ExtractExpression& extract_expression) {
  Fail("Only Strings (YYYY-MM-DD) supported for Dates right now");
}

template <size_t offset, size_t count>
std::shared_ptr<ExpressionResult<pmr_string>> ExpressionEvaluator::_evaluate_extract_substr(
    const ExpressionResult<pmr_string>& from_result) {
  std::shared_ptr<ExpressionResult<pmr_string>> result;

  std::vector<pmr_string> values(from_result.size());

  from_result.as_view([&](const auto& from_view) {
    for (auto chunk_offset = ChunkOffset{0}; chunk_offset < static_cast<ChunkOffset>(from_view.size());
         ++chunk_offset) {
      if (!from_view.is_null(chunk_offset)) {
        DebugAssert(from_view.value(chunk_offset).size() == 10u,
                    "Invalid DatetimeString '"s + std::string{from_view.value(chunk_offset)} + "'");  // NOLINT
        values[chunk_offset] = from_view.value(chunk_offset).substr(offset, count);
      }
    }
  });

  return std::make_shared<ExpressionResult<pmr_string>>(std::move(values), from_result.nulls);
}

template <typename Result>
std::shared_ptr<ExpressionResult<Result>> ExpressionEvaluator::_evaluate_unary_minus_expression(
    const UnaryMinusExpression& unary_minus_expression) {
  std::vector<Result> values;
  std::vector<bool> nulls;

  _resolve_to_expression_result(*unary_minus_expression.argument(), [&](const auto& argument_result) {
    using ArgumentType = typename std::decay_t<decltype(argument_result)>::Type;

    if constexpr (!std::is_same_v<ArgumentType, pmr_string> && std::is_same_v<Result, ArgumentType>) {
      values.resize(argument_result.size());
      for (auto chunk_offset = ChunkOffset{0}; chunk_offset < static_cast<ChunkOffset>(argument_result.size());
           ++chunk_offset) {
        // NOTE: Actual negation happens in this line
        values[chunk_offset] = -argument_result.values[chunk_offset];
      }
      nulls = argument_result.nulls;
    } else {
      Fail("Can't negate a Strings, can't negate an argument to a different type");
    }
  });

  return std::make_shared<ExpressionResult<Result>>(std::move(values), std::move(nulls));
}

template <typename Result>
std::shared_ptr<ExpressionResult<Result>> ExpressionEvaluator::_evaluate_subquery_expression(
    const PQPSubqueryExpression& subquery_expression) {
  // One table per row. Each table should have a single row with a single value
  const auto subquery_result_tables = _evaluate_subquery_expression_to_tables(subquery_expression);

  // One ExpressionResult<Result> per row. Each ExpressionResult<Result> should have a single value
  const auto subquery_results = _prune_tables_to_expression_results<Result>(subquery_result_tables);

  std::vector<Result> result_values(subquery_results.size());
  std::vector<bool> result_nulls;

  // Materialize values
  for (auto chunk_offset = ChunkOffset{0}; chunk_offset < static_cast<ChunkOffset>(subquery_results.size());
       ++chunk_offset) {
    Assert(subquery_results[chunk_offset]->size() == 1,
           "Expected precisely one row to be returned from SelectExpression");
    result_values[chunk_offset] = subquery_results[chunk_offset]->value(0);
  }

  // Optionally materialize nulls if any row returned a nullable result.
  const auto nullable = std::any_of(subquery_results.begin(), subquery_results.end(),
                                    [&](const auto& expression_result) { return expression_result->is_nullable(); });

  if (nullable) {
    result_nulls.resize(subquery_results.size());
    for (auto chunk_offset = ChunkOffset{0}; chunk_offset < static_cast<ChunkOffset>(subquery_results.size());
         ++chunk_offset) {
      result_nulls[chunk_offset] = subquery_results[chunk_offset]->is_null(0);
    }
  }

  return std::make_shared<ExpressionResult<Result>>(std::move(result_values), std::move(result_nulls));
}

std::vector<std::shared_ptr<const Table>> ExpressionEvaluator::_evaluate_subquery_expression_to_tables(
    const PQPSubqueryExpression& expression) {
  // If the SubqueryExpression is uncorrelated, evaluating it once is sufficient
  if (expression.parameters.empty()) {
    if (_uncorrelated_subquery_results) {
      // This was already evaluated before
      const auto table_iter = _uncorrelated_subquery_results->find(expression.pqp);
      DebugAssert(table_iter != _uncorrelated_subquery_results->cend(),
                  "All uncorrelated PQPSubqueryExpression should be cached, if cache is present");
      return {table_iter->second};
    } else {
      // If a subquery is uncorrelated, it has the same result for all rows, so we just execute it for the first row
      return {_evaluate_subquery_expression_for_row(expression, ChunkOffset{0})};
    }
  }

  // Make sure all columns (i.e. segments) that are parameters are materialized
  for (const auto& parameter : expression.parameters) {
    _materialize_segment_if_not_yet_materialized(parameter.second);
  }

  std::vector<std::shared_ptr<const Table>> results(_output_row_count);

  for (auto chunk_offset = ChunkOffset{0}; chunk_offset < static_cast<ChunkOffset>(_output_row_count); ++chunk_offset) {
    results[chunk_offset] = _evaluate_subquery_expression_for_row(expression, chunk_offset);
  }

  return results;
}

std::shared_ptr<ExpressionEvaluator::UncorrelatedSubqueryResults>
ExpressionEvaluator::populate_uncorrelated_subquery_results_cache(
    const std::vector<std::shared_ptr<AbstractExpression>>& expressions) {
  auto uncorrelated_subquery_results = std::make_shared<ExpressionEvaluator::UncorrelatedSubqueryResults>();
  auto evaluator = ExpressionEvaluator{};
  for (const auto& expression : expressions) {
    visit_expression(expression, [&](const auto& sub_expression) {
      const auto pqp_subquery_expression = std::dynamic_pointer_cast<PQPSubqueryExpression>(sub_expression);
      if (pqp_subquery_expression && !pqp_subquery_expression->is_correlated()) {
        // Uncorrelated subquery expressions have the same result for every row, so executing them for row 0 is fine.
        auto result = evaluator._evaluate_subquery_expression_for_row(*pqp_subquery_expression, ChunkOffset{0});
        uncorrelated_subquery_results->emplace(pqp_subquery_expression->pqp, std::move(result));
        return ExpressionVisitation::DoNotVisitArguments;
      }

      return ExpressionVisitation::VisitArguments;
    });
  }
  return uncorrelated_subquery_results;
}

std::shared_ptr<const Table> ExpressionEvaluator::_evaluate_subquery_expression_for_row(
    const PQPSubqueryExpression& expression, const ChunkOffset chunk_offset) {
  Assert(expression.parameters.empty() || _chunk,
         "Sub-SELECT references external Columns but Expression doesn't operate on a Table/Chunk");

  std::unordered_map<ParameterID, AllTypeVariant> parameters;

  for (auto parameter_idx = size_t{0}; parameter_idx < expression.parameters.size(); ++parameter_idx) {
    const auto& parameter_id_column_id = expression.parameters[parameter_idx];
    const auto parameter_id = parameter_id_column_id.first;
    const auto column_id = parameter_id_column_id.second;

    const auto value = _segment_materializations[column_id]->value_as_variant(chunk_offset);

    parameters.emplace(parameter_id, value);
  }

  // TODO(moritz) deep_copy() shouldn't be necessary for every row if we could re-execute PQPs...
  auto row_pqp = expression.pqp->deep_copy();
  row_pqp->set_parameters(parameters);

  const auto tasks = OperatorTask::make_tasks_from_operator(row_pqp, CleanupTemporaries::Yes);
  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(tasks);

  return row_pqp->get_output();
}

std::shared_ptr<BaseValueSegment> ExpressionEvaluator::evaluate_expression_to_segment(
    const AbstractExpression& expression) {
  std::shared_ptr<BaseValueSegment> segment;
  std::vector<bool> nulls;

  _resolve_to_expression_result_view(expression, [&](const auto& view) {
    using ColumnDataType = typename std::decay_t<decltype(view)>::Type;

    if constexpr (std::is_same_v<ColumnDataType, NullValue>) {
      Fail("Can't create a Segment from a NULL");
    } else {
      std::vector<ColumnDataType> values(_output_row_count);

      for (auto chunk_offset = ChunkOffset{0}; chunk_offset < static_cast<ChunkOffset>(_output_row_count);
           ++chunk_offset) {
        values[chunk_offset] = std::move(view.value(chunk_offset));
      }

      if (view.is_nullable()) {
        nulls.resize(_output_row_count);
        for (auto chunk_offset = ChunkOffset{0}; chunk_offset < static_cast<ChunkOffset>(_output_row_count);
             ++chunk_offset) {
          nulls[chunk_offset] = view.is_null(chunk_offset);
        }
        segment = std::make_shared<ValueSegment<ColumnDataType>>(std::move(values), std::move(nulls));
      } else {
        segment = std::make_shared<ValueSegment<ColumnDataType>>(std::move(values));
      }
    }
  });

  return segment;
}

PosList ExpressionEvaluator::evaluate_expression_to_pos_list(const AbstractExpression& expression) {
  /**
   * Only Expressions returning a Bool can be evaluated to a PosList of matches.
   *
   * (Not)In and (Not)Like Expressions are evaluated by generating an ExpressionResult of booleans
   * (evaluate_expression_to_result<>()) which is then scanned for positive entries.
   * TODO(anybody) Add fast implementations for (Not)In and (Not)Like as well.
   *
   * All other Expression types have dedicated, hopefully fast, implementations.
   */

  auto result_pos_list = PosList{};

  switch (expression.type) {
    case ExpressionType::Predicate: {
      const auto& predicate_expression = static_cast<const AbstractPredicateExpression&>(expression);

      // To reduce the number of template instantiations, we flip > and >= to < and <=
      bool flip = false;
      auto predicate_condition = predicate_expression.predicate_condition;

      switch (predicate_expression.predicate_condition) {
        case PredicateCondition::GreaterThanEquals:
        case PredicateCondition::GreaterThan:
          flip = true;
          predicate_condition = flip_predicate_condition(predicate_condition);
          [[fallthrough]];

        case PredicateCondition::Equals:
        case PredicateCondition::LessThanEquals:
        case PredicateCondition::NotEquals:
        case PredicateCondition::LessThan: {
          const auto& left = *predicate_expression.arguments[flip ? 1 : 0];
          const auto& right = *predicate_expression.arguments[flip ? 0 : 1];

          _resolve_to_expression_results(left, right, [&](const auto& left_result, const auto& right_result) {
            using LeftDataType = typename std::decay_t<decltype(left_result)>::Type;
            using RightDataType = typename std::decay_t<decltype(right_result)>::Type;

            resolve_binary_predicate_evaluator(predicate_condition, [&](const auto functor) {
              using ExpressionFunctorType = typename decltype(functor)::type;

              if constexpr (ExpressionFunctorType::template supports<ExpressionEvaluator::Bool, LeftDataType,
                                                                     RightDataType>::value) {
                for (auto chunk_offset = ChunkOffset{0}; chunk_offset < static_cast<ChunkOffset>(_output_row_count);
                     ++chunk_offset) {
                  if (left_result.is_null(chunk_offset) || right_result.is_null(chunk_offset)) continue;

                  auto matches = ExpressionEvaluator::Bool{0};
                  ExpressionFunctorType{}(matches, left_result.value(chunk_offset),  // NOLINT
                                          right_result.value(chunk_offset));
                  if (matches != 0) result_pos_list.emplace_back(RowID{_chunk_id, chunk_offset});
                }
              } else {
                Fail("Argument types not compatible");
              }
            });
          });
        } break;

        case PredicateCondition::BetweenInclusive:
        case PredicateCondition::BetweenLowerExclusive:
        case PredicateCondition::BetweenUpperExclusive:
        case PredicateCondition::BetweenExclusive:
          return evaluate_expression_to_pos_list(*rewrite_between_expression(expression));

        case PredicateCondition::IsNull:
        case PredicateCondition::IsNotNull: {
          const auto& is_null_expression = static_cast<const IsNullExpression&>(expression);

          _resolve_to_expression_result_view(*is_null_expression.operand(), [&](const auto& result) {
            if (is_null_expression.predicate_condition == PredicateCondition::IsNull) {
              for (auto chunk_offset = ChunkOffset{0}; chunk_offset < static_cast<ChunkOffset>(_output_row_count);
                   ++chunk_offset) {
                if (result.is_null(chunk_offset)) result_pos_list.emplace_back(RowID{_chunk_id, chunk_offset});
              }
            } else {  // PredicateCondition::IsNotNull
              for (auto chunk_offset = ChunkOffset{0}; chunk_offset < static_cast<ChunkOffset>(_output_row_count);
                   ++chunk_offset) {
                if (!result.is_null(chunk_offset)) result_pos_list.emplace_back(RowID{_chunk_id, chunk_offset});
              }
            }
          });
        } break;

        case PredicateCondition::In:
        case PredicateCondition::NotIn:
        case PredicateCondition::Like:
        case PredicateCondition::NotLike: {
          // Evaluating (Not)In and (Not)Like to PosLists uses evaluate_expression_to_result() and scans the Series
          // it returns for matches. This is probably slower than a dedicated evaluate-to-PosList implementation
          // for these ExpressionTypes could be. But
          // a) such implementations would require lots of code, there is little potential for code sharing between the
          //    evaluate-to-PosList and evaluate-to-Result implementations
          // b) Like/In are on the slower end anyway
          const auto result = evaluate_expression_to_result<ExpressionEvaluator::Bool>(expression);
          result->as_view([&](const auto& result_view) {
            for (auto chunk_offset = ChunkOffset{0}; chunk_offset < static_cast<ChunkOffset>(_output_row_count);
                 ++chunk_offset) {
              if (result_view.value(chunk_offset) != 0 && !result_view.is_null(chunk_offset)) {
                result_pos_list.emplace_back(RowID{_chunk_id, chunk_offset});
              }
            }
          });
        } break;
      }
    } break;

    case ExpressionType::Logical: {
      const auto& logical_expression = static_cast<const LogicalExpression&>(expression);

      const auto left_pos_list = evaluate_expression_to_pos_list(*logical_expression.arguments[0]);
      const auto right_pos_list = evaluate_expression_to_pos_list(*logical_expression.arguments[1]);

      switch (logical_expression.logical_operator) {
        case LogicalOperator::And:
          std::set_intersection(left_pos_list.begin(), left_pos_list.end(), right_pos_list.begin(),
                                right_pos_list.end(), std::back_inserter(result_pos_list));
          break;

        case LogicalOperator::Or:
          std::set_union(left_pos_list.begin(), left_pos_list.end(), right_pos_list.begin(), right_pos_list.end(),
                         std::back_inserter(result_pos_list));
          break;
      }
    } break;

    case ExpressionType::Exists: {
      const auto& exists_expression = static_cast<const ExistsExpression&>(expression);
      const auto subquery_expression = std::dynamic_pointer_cast<PQPSubqueryExpression>(exists_expression.subquery());
      Assert(subquery_expression, "Expected PQPSubqueryExpression");

      const auto invert = exists_expression.exists_expression_type == ExistsExpressionType::NotExists;

      const auto subquery_result_tables = _evaluate_subquery_expression_to_tables(*subquery_expression);
      if (subquery_expression->is_correlated()) {
        for (auto chunk_offset = ChunkOffset{0}; chunk_offset < static_cast<ChunkOffset>(_output_row_count);
             ++chunk_offset) {
          if ((subquery_result_tables[chunk_offset]->row_count() > 0) ^ invert) {
            result_pos_list.emplace_back(RowID{_chunk_id, chunk_offset});
          }
        }
      } else {
        if ((subquery_result_tables.front()->row_count() > 0) ^ invert) {
          for (auto chunk_offset = ChunkOffset{0}; chunk_offset < static_cast<ChunkOffset>(_output_row_count);
               ++chunk_offset) {
            result_pos_list.emplace_back(RowID{_chunk_id, chunk_offset});
          }
        }
      }
    } break;

    // Boolean literals
    case ExpressionType::Value: {
      const auto& value_expression = static_cast<const ValueExpression&>(expression);
      Assert(value_expression.value.type() == typeid(ExpressionEvaluator::Bool),
             "Cannot evaluate non-boolean literal to PosList");
      // TRUE literal returns the entire Chunk, FALSE literal returns empty PosList
      if (boost::get<ExpressionEvaluator::Bool>(value_expression.value) != 0) {
        result_pos_list.resize(_output_row_count);
        for (auto chunk_offset = ChunkOffset{0}; chunk_offset < static_cast<ChunkOffset>(_output_row_count);
             ++chunk_offset) {
          result_pos_list[chunk_offset] = {_chunk_id, chunk_offset};
        }
      }
    } break;

    default:
      Fail("Expression type cannot be evaluated to PosList");
  }

  return result_pos_list;
}

template <>
std::shared_ptr<ExpressionResult<ExpressionEvaluator::Bool>>
ExpressionEvaluator::_evaluate_logical_expression<ExpressionEvaluator::Bool>(const LogicalExpression& expression) {
  const auto& left = *expression.left_operand();
  const auto& right = *expression.right_operand();

  // clang-format off
  switch (expression.logical_operator) {
    case LogicalOperator::Or:  return _evaluate_binary_with_functor_based_null_logic<ExpressionEvaluator::Bool, TernaryOrEvaluator>(left, right);  // NOLINT
    case LogicalOperator::And: return _evaluate_binary_with_functor_based_null_logic<ExpressionEvaluator::Bool, TernaryAndEvaluator>(left, right);  // NOLINT
  }
  // clang-format on

  Fail("Invalid enum value");
}

template <typename Result>
std::shared_ptr<ExpressionResult<Result>> ExpressionEvaluator::_evaluate_logical_expression(
    const LogicalExpression& expression) {
  Fail("LogicalExpression can only output bool");
}

template <typename Result, typename Functor>
std::shared_ptr<ExpressionResult<Result>> ExpressionEvaluator::_evaluate_binary_with_default_null_logic(
    const AbstractExpression& left_expression, const AbstractExpression& right_expression) {
  std::vector<Result> values;
  std::vector<bool> nulls;

  _resolve_to_expression_results(left_expression, right_expression, [&](const auto& left, const auto& right) {
    using LeftDataType = typename std::decay_t<decltype(left)>::Type;
    using RightDataType = typename std::decay_t<decltype(right)>::Type;

    if constexpr (Functor::template supports<Result, LeftDataType, RightDataType>::value) {
      const auto result_size = _result_size(left.size(), right.size());
      values.resize(result_size);
      nulls = _evaluate_default_null_logic(left.nulls, right.nulls);

      // Using three different branches instead of views, which would generate 9 cases.
      if (left.is_literal() == right.is_literal()) {
        for (auto row_idx = ChunkOffset{0}; row_idx < result_size; ++row_idx) {
          Functor{}(values[row_idx], left.values[row_idx], right.values[row_idx]);
        }
      } else if (right.is_literal()) {
        for (auto row_idx = ChunkOffset{0}; row_idx < result_size; ++row_idx) {
          Functor{}(values[row_idx], left.values[row_idx], right.values.front());
        }
      } else {
        for (auto row_idx = ChunkOffset{0}; row_idx < result_size; ++row_idx) {
          Functor{}(values[row_idx], left.values.front(), right.values[row_idx]);
        }
      }
    } else {
      Fail("BinaryOperation not supported on the requested DataTypes");
    }
  });

  return std::make_shared<ExpressionResult<Result>>(std::move(values), std::move(nulls));
}

template <typename Result, typename Functor>
std::shared_ptr<ExpressionResult<Result>> ExpressionEvaluator::_evaluate_binary_with_functor_based_null_logic(
    const AbstractExpression& left_expression, const AbstractExpression& right_expression) {
  auto result = std::shared_ptr<ExpressionResult<Result>>{};

  _resolve_to_expression_result_views(left_expression, right_expression, [&](const auto& left, const auto& right) {
    using LeftDataType = typename std::decay_t<decltype(left)>::Type;
    using RightDataType = typename std::decay_t<decltype(right)>::Type;

    if constexpr (Functor::template supports<Result, LeftDataType, RightDataType>::value) {
      const auto result_row_count = _result_size(left.size(), right.size());

      std::vector<bool> nulls(result_row_count);
      std::vector<Result> values(result_row_count);

      for (auto row_idx = ChunkOffset{0}; row_idx < result_row_count; ++row_idx) {
        bool null;
        Functor{}(values[row_idx], null, left.value(row_idx), left.is_null(row_idx), right.value(row_idx),
                  right.is_null(row_idx));
        nulls[row_idx] = null;
      }

      result = std::make_shared<ExpressionResult<Result>>(std::move(values), std::move(nulls));

    } else {
      Fail("BinaryOperation not supported on the requested DataTypes");
    }
  });

  return result;
}

template <typename Functor>
void ExpressionEvaluator::_resolve_to_expression_result_view(const AbstractExpression& expression, const Functor& fn) {
  _resolve_to_expression_result(expression,
                                [&](const auto& result) { result.as_view([&](const auto& view) { fn(view); }); });
}

template <typename Functor>
void ExpressionEvaluator::_resolve_to_expression_result_views(const AbstractExpression& left_expression,
                                                              const AbstractExpression& right_expression,
                                                              const Functor& fn) {
  _resolve_to_expression_results(left_expression, right_expression,
                                 [&](const auto& left_result, const auto& right_result) {
                                   left_result.as_view([&](const auto& left_view) {
                                     right_result.as_view([&](const auto& right_view) { fn(left_view, right_view); });
                                   });
                                 });
}

template <typename Functor>
void ExpressionEvaluator::_resolve_to_expression_results(const AbstractExpression& left_expression,
                                                         const AbstractExpression& right_expression,
                                                         const Functor& fn) {
  _resolve_to_expression_result(left_expression, [&](const auto& left_result) {
    _resolve_to_expression_result(right_expression, [&](const auto& right_result) { fn(left_result, right_result); });
  });
}

template <typename Functor>
void ExpressionEvaluator::_resolve_to_expression_result(const AbstractExpression& expression, const Functor& fn) {
  Assert(expression.type != ExpressionType::List, "Can't resolve ListExpression to ExpressionResult");

  if (expression.data_type() == DataType::Null) {
    // resolve_data_type() doesn't support Null, so we have handle it explicitly
    ExpressionResult<NullValue> null_value_result{{NullValue{}}, {true}};

    fn(null_value_result);

  } else {
    resolve_data_type(expression.data_type(), [&](const auto data_type_t) {
      using ExpressionDataType = typename decltype(data_type_t)::type;

      const auto expression_result = evaluate_expression_to_result<ExpressionDataType>(expression);
      fn(*expression_result);
    });
  }
}

template <typename... RowCounts>
ChunkOffset ExpressionEvaluator::_result_size(const RowCounts... row_counts) {
  // If any operand is empty (that's the case IFF it is an empty segment) the result of the expression has no rows
  //
  //  _result_size() covers the following scenarios:
  //    - Column-involving expression evaluation on an empty Chunk should give you zero rows.
  //        So a + 5 should be empty on an empty Chunk.
  //    - If the Chunk is NOT empty, Literal-and-Column involving expression evaluation should give you one result per
  //        row, so a + 5 should give you one value for each element in a.
  //    - Non-column involving expressions should give you one result value,
  //        no matter whether there is a (potentially) non-empty Chunk involved or not.
  //        So 5+3 always gives you one result element: 8

  if (((row_counts == 0) || ...)) return 0;

  return static_cast<ChunkOffset>(std::max({row_counts...}));
}

std::vector<bool> ExpressionEvaluator::_evaluate_default_null_logic(const std::vector<bool>& left,
                                                                    const std::vector<bool>& right) {
  if (left.size() == right.size()) {
    std::vector<bool> nulls(left.size());
    std::transform(left.begin(), left.end(), right.begin(), nulls.begin(), [](auto l, auto r) { return l || r; });
    return nulls;
  } else if (left.size() > right.size()) {
    DebugAssert(right.size() <= 1,
                "Operand should have either the same row count as the other, 1 row (to represent a literal), or no "
                "rows (to represent a non-nullable operand)");
    if (!right.empty() && right.front()) {
      return std::vector<bool>({true});
    } else {
      return left;
    }
  } else {
    DebugAssert(left.size() <= 1,
                "Operand should have either the same row count as the other, 1 row (to represent a literal), or no "
                "rows (to represent a non-nullable operand)");
    if (!left.empty() && left.front()) {
      return std::vector<bool>({true});
    } else {
      return right;
    }
  }
}

void ExpressionEvaluator::_materialize_segment_if_not_yet_materialized(const ColumnID column_id) {
  Assert(_chunk, "Cannot access columns in this Expression as it doesn't operate on a Table/Chunk");

  if (_segment_materializations[column_id]) return;

  const auto& segment = *_chunk->get_segment(column_id);

  resolve_data_type(segment.data_type(), [&](const auto column_data_type_t) {
    using ColumnDataType = typename decltype(column_data_type_t)::type;

    std::vector<ColumnDataType> values(segment.size());

    auto chunk_offset = ChunkOffset{0};

    if (_table->column_is_nullable(column_id)) {
      std::vector<bool> nulls(segment.size());

      segment_iterate<ColumnDataType>(segment, [&](const auto& position) {
        if (position.is_null()) {
          nulls[chunk_offset] = true;
        } else {
          values[chunk_offset] = position.value();
        }
        ++chunk_offset;
      });

      _segment_materializations[column_id] =
          std::make_shared<ExpressionResult<ColumnDataType>>(std::move(values), std::move(nulls));

    } else {
      segment_iterate<ColumnDataType>(segment, [&](const auto& position) {
        DebugAssert(!position.is_null(), "Encountered NULL value in non-nullable column");
        values[chunk_offset] = position.value();
        ++chunk_offset;
      });

      _segment_materializations[column_id] = std::make_shared<ExpressionResult<ColumnDataType>>(std::move(values));
    }
  });
}

std::shared_ptr<ExpressionResult<pmr_string>> ExpressionEvaluator::_evaluate_substring(
    const std::vector<std::shared_ptr<AbstractExpression>>& arguments) {
  DebugAssert(arguments.size() == 3, "SUBSTR expects three arguments");

  const auto strings = evaluate_expression_to_result<pmr_string>(*arguments[0]);
  const auto starts = evaluate_expression_to_result<int32_t>(*arguments[1]);
  const auto lengths = evaluate_expression_to_result<int32_t>(*arguments[2]);

  const auto row_count = _result_size(strings->size(), starts->size(), lengths->size());

  std::vector<pmr_string> result_values(row_count);
  std::vector<bool> result_nulls(row_count);

  for (auto chunk_offset = ChunkOffset{0}; chunk_offset < static_cast<ChunkOffset>(row_count); ++chunk_offset) {
    result_nulls[chunk_offset] =
        strings->is_null(chunk_offset) || starts->is_null(chunk_offset) || lengths->is_null(chunk_offset);

    const auto& string = strings->value(chunk_offset);
    DebugAssert(
        string.size() < size_t{std::numeric_limits<int32_t>::max()},
        "String is too long to be handled by SUBSTR. Switch to int64_t in the SUBSTR implementation if you really "
        "need to.");

    const auto signed_string_size = static_cast<int32_t>(string.size());

    auto length = lengths->value(chunk_offset);
    if (length <= 0) continue;

    auto start = starts->value(chunk_offset);

    /**
     * Hyrise SUBSTR follows SQLite semantics for negative indices. SUBSTR lives in this weird "space" illustrated below
     * Note that other DBMS behave differently when it comes to negative indices.
     *
     * START -8 -7 -6 -5 -4 -3 -2 -1 || 0  || 1 2 3 4 5 6  7  8
     * CHAR  // // // H  e  l  l  o  || // || H e l l o // // //
     *
     * SUBSTR('HELLO', 0, 2) -> 'H'
     * SUBSTR('HELLO', -1, 2) -> 'O'
     * SUBSTR('HELLO', -8, 1) -> ''
     * SUBSTR('HELLO', -8, 5) -> 'HE'
     */
    auto end = int32_t{0};
    if (start < 0) {
      start += signed_string_size;
    } else {
      if (start == 0) {
        length -= 1;
      } else {
        start -= 1;
      }
    }

    end = start + length;
    start = std::max(0, start);
    end = std::min(end, signed_string_size);
    length = end - start;

    // Invalid/out of range arguments, like SUBSTR("HELLO", 4000, -2), lead to an empty string
    if (!string.empty() && start >= 0 && start < signed_string_size && length > 0) {
      length = std::min<int32_t>(signed_string_size - start, length);
      result_values[chunk_offset] = string.substr(static_cast<size_t>(start), static_cast<size_t>(length));
    }
  }

  return std::make_shared<ExpressionResult<pmr_string>>(result_values, result_nulls);
}

std::shared_ptr<ExpressionResult<pmr_string>> ExpressionEvaluator::_evaluate_concatenate(
    const std::vector<std::shared_ptr<AbstractExpression>>& arguments) {
  /**
   * Emulates SQLite's CONCAT() - e.g. returning NULL once any argument is NULL
   */

  std::vector<std::shared_ptr<ExpressionResult<pmr_string>>> argument_results;
  argument_results.reserve(arguments.size());

  auto result_is_nullable = false;

  // 1 - Compute the arguments
  for (const auto& argument : arguments) {
    // CONCAT with a NULL literal argument -> result is NULL
    if (argument->data_type() == DataType::Null) {
      auto null_value_result = ExpressionResult<pmr_string>{{pmr_string{}}, {true}};
      return std::make_shared<ExpressionResult<pmr_string>>(null_value_result);
    }

    const auto argument_result = evaluate_expression_to_result<pmr_string>(*argument);
    argument_results.emplace_back(argument_result);

    result_is_nullable |= argument_result->is_nullable();
  }

  // 2 - Compute the number of output rows
  auto result_size = argument_results.empty() ? size_t{0} : argument_results.front()->size();
  for (auto argument_idx = size_t{1}; argument_idx < argument_results.size(); ++argument_idx) {
    result_size = _result_size(result_size, argument_results[argument_idx]->size());
  }

  // 3 - Concatenate the values
  std::vector<pmr_string> result_values(result_size);
  for (const auto& argument_result : argument_results) {
    argument_result->as_view([&](const auto& argument_view) {
      for (auto chunk_offset = ChunkOffset{0}; chunk_offset < static_cast<ChunkOffset>(result_size); ++chunk_offset) {
        // The actual CONCAT
        result_values[chunk_offset] += argument_view.value(chunk_offset);
      }
    });
  }

  // 4 - Optionally concatenate the nulls (i.e. one argument is null -> result is null) and return
  std::vector<bool> result_nulls{};
  if (result_is_nullable) {
    result_nulls.resize(result_size, false);
    for (const auto& argument_result : argument_results) {
      argument_result->as_view([&](const auto& argument_view) {
        for (auto chunk_offset = ChunkOffset{0}; chunk_offset < static_cast<ChunkOffset>(result_size); ++chunk_offset) {
          // This was `result_nulls[chunk_offset] = result_nulls[chunk_offset] || argument_view.is_null(chunk_offset);`
          // but valgrind reported access to uninitialized memory in release builds (and ONLY in them!). I can't see
          // how there was anything uninitialised given the `result_nulls.resize(result_size, false);` above.
          // Anyway, changing it to the line below silences valgrind.
          if (argument_view.is_null(chunk_offset)) result_nulls[chunk_offset] = true;
        }
      });
    }
  }

  return std::make_shared<ExpressionResult<pmr_string>>(std::move(result_values), std::move(result_nulls));
}

template <typename Result>
std::vector<std::shared_ptr<ExpressionResult<Result>>> ExpressionEvaluator::_prune_tables_to_expression_results(
    const std::vector<std::shared_ptr<const Table>>& tables) {
  /**
   * Makes sure each Table in @param tables has only a single column. Materialize this single column into
   * an ExpressionResult and return the vector of resulting ExpressionResults.
   */

  std::vector<std::shared_ptr<ExpressionResult<Result>>> results(tables.size());

  for (auto table_idx = size_t{0}; table_idx < tables.size(); ++table_idx) {
    const auto& table = tables[table_idx];

    Assert(table->column_count() == 1, "Expected precisely one column from Subquery");
    Assert(table->column_data_type(ColumnID{0}) == data_type_from_type<Result>(),
           "Expected different DataType from Subquery");

    std::vector<bool> result_nulls;
    std::vector<Result> result_values(table->row_count());

    auto chunk_offset = ChunkOffset{0};

    if (table->column_is_nullable(ColumnID{0})) {
      result_nulls.resize(table->row_count());

      const auto chunk_count = table->chunk_count();
      for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
        const auto chunk = table->get_chunk(chunk_id);
        Assert(chunk, "Did not expect deleted chunk here.");  // see #1686

        const auto& result_segment = *chunk->get_segment(ColumnID{0});
        segment_iterate<Result>(result_segment, [&](const auto& position) {
          if (position.is_null()) {
            result_nulls[chunk_offset] = true;
          } else {
            result_values[chunk_offset] = position.value();
          }
          ++chunk_offset;
        });
      }
    } else {
      const auto chunk_count = table->chunk_count();
      for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
        const auto chunk = table->get_chunk(chunk_id);
        Assert(chunk, "Did not expect deleted chunk here.");  // see #1686

        const auto& result_segment = *chunk->get_segment(ColumnID{0});
        segment_iterate<Result>(result_segment, [&](const auto& position) {
          result_values[chunk_offset] = position.value();
          ++chunk_offset;
        });
      }
    }

    results[table_idx] = std::make_shared<ExpressionResult<Result>>(std::move(result_values), std::move(result_nulls));
  }

  return results;
}

}  // namespace opossum
