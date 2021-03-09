#pragma once

#include <functional>
#include <memory>
#include <queue>
#include <unordered_map>
#include <vector>

#include "abstract_expression.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "operators/abstract_operator.hpp"

namespace opossum {

class AbstractLQPNode;
class LQPColumnExpression;
class TransactionContext;
class PQPSubqueryExpression;
enum class LogicalOperator;

/**
 * Utility to check whether two vectors of Expressions are equal according to AbstractExpression::operator==()
 * Note that this function also pays respect to the order of elements.
 */
bool expressions_equal(const std::vector<std::shared_ptr<AbstractExpression>>& expressions_a,
                       const std::vector<std::shared_ptr<AbstractExpression>>& expressions_b);

/**
 * Utility to compare vectors of Expressions from different LQPs
 */
bool expressions_equal_to_expressions_in_different_lqp(
    const std::vector<std::shared_ptr<AbstractExpression>>& expressions_left,
    const std::vector<std::shared_ptr<AbstractExpression>>& expressions_right, const LQPNodeMapping& node_mapping);

/**
 * Utility to compare two Expressions from different LQPs
 */
bool expression_equal_to_expression_in_different_lqp(const AbstractExpression& expression_left,
                                                     const AbstractExpression& expression_right,
                                                     const LQPNodeMapping& node_mapping);

/**
 * Utility to AbstractExpression::deep_copy() a vector of expressions.
 *
 * Regarding PQPSubqueryExpressions: Deduplication of operator plans will be preserved. See lqp_translator.cpp
 * for more info on deduplication.
 */
std::vector<std::shared_ptr<AbstractExpression>> expressions_deep_copy(
    const std::vector<std::shared_ptr<AbstractExpression>>& expressions);

/**
 * Utility to AbstractExpression::deep_copy() a vector of expressions. Uses
 * @param copied_ops to preserve deduplication for the operator plans of PQPSubqueryExpressions.
 * See lqp_translator.cpp for more info on deduplication.
 */
std::vector<std::shared_ptr<AbstractExpression>> expressions_deep_copy(
    const std::vector<std::shared_ptr<AbstractExpression>>& expressions,
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops);

/**
 * Recurse through the expression and replace them according to @param mapping, where applicable
 */
void expression_deep_replace(std::shared_ptr<AbstractExpression>& expression,
                             const ExpressionUnorderedMap<std::shared_ptr<AbstractExpression>>& mapping);

/**
 * Utility to AbstractExpression::deep_copy() a vector of expressions while adjusting LQPColumnExpressions according to
 * the node_mapping
 */
std::vector<std::shared_ptr<AbstractExpression>> expressions_copy_and_adapt_to_different_lqp(
    const std::vector<std::shared_ptr<AbstractExpression>>& expressions, const LQPNodeMapping& node_mapping);

/**
 * Utility to AbstractExpression::deep_copy() a single expression while adjusting LQPColumnExpressions according to the
 * node_mapping
 */
std::shared_ptr<AbstractExpression> expression_copy_and_adapt_to_different_lqp(const AbstractExpression& expression,
                                                                               const LQPNodeMapping& node_mapping);

/**
 * Makes all LQPColumnExpressions point to their equivalent in a copied LQP
 */
void expression_adapt_to_different_lqp(std::shared_ptr<AbstractExpression>& expression,
                                       const LQPNodeMapping& node_mapping);

std::shared_ptr<LQPColumnExpression> expression_adapt_to_different_lqp(const LQPColumnExpression& lqp_column_expression,
                                                                       const LQPNodeMapping& node_mapping);

/**
 * Create a comma separated string with the AbstractExpression::description(mode) of each expression
 */
std::string expression_descriptions(const std::vector<std::shared_ptr<AbstractExpression>>& expressions,
                                    const AbstractExpression::DescriptionMode mode);

enum class ExpressionVisitation { VisitArguments, DoNotVisitArguments };

/**
 * Calls the passed @param visitor on each sub-expression of the @param expression.
 * The visitor returns `ExpressionVisitation`, indicating whether the current expression's arguments should be visited
 * as well.
 *
 * @tparam Expression   Either `std::shared_ptr<AbstractExpression>` or `const std::shared_ptr<AbstractExpression>`
 * @tparam Visitor      Functor called with every sub expression as a param.
 *                      Return `ExpressionVisitation`
 */
template <typename Expression, typename Visitor>
void visit_expression(Expression& expression, Visitor visitor) {
  // The reference wrapper bit is important so we can manipulate the Expression even by replacing sub expression
  std::queue<std::reference_wrapper<Expression>> expression_queue;
  expression_queue.push(expression);

  while (!expression_queue.empty()) {
    auto expression_reference = expression_queue.front();
    expression_queue.pop();

    if (visitor(expression_reference.get()) == ExpressionVisitation::VisitArguments) {
      for (auto& argument : expression_reference.get()->arguments) {
        expression_queue.push(argument);
      }
    }
  }
}

/**
 * @return  The result DataType of a non-boolean binary expression where the operands have the specified types.
 *          E.g., `<float> + <long> => <double>`, `(<float>, <int>, <int>) => <float>`
 *          Division of integer types will return an integer type, see #1799.
 */
DataType expression_common_type(const DataType lhs, const DataType rhs);

/**
 * @return Checks whether the expression can be evaluated on top of a specified LQP (i.e., all required
 *         LQPColumnExpressions are available from this LQP). This does not mean that all expressions are already
 *         readily available as a column. It might be necessary to add a Projection or an Aggregate.
 *         To check if an expression is available in a form ready to be used by a scan/join,
 *         use `Operator*Predicate::from_expression(...)`.
 */
bool expression_evaluable_on_lqp(const std::shared_ptr<AbstractExpression>& expression, const AbstractLQPNode& lqp);

/**
 * Convert "(a AND b) AND c" to [a,b,c] where a,b,c can be arbitrarily complex expressions
 */
std::vector<std::shared_ptr<AbstractExpression>> flatten_logical_expressions(
    const std::shared_ptr<AbstractExpression>& expression, const LogicalOperator logical_operator);

/**
 * Convert ([a,b,c], AND) into "(a AND b) AND c"
 */
std::shared_ptr<AbstractExpression> inflate_logical_expressions(
    const std::vector<std::shared_ptr<AbstractExpression>>& expressions, const LogicalOperator logical_operator);

/**
 * Traverse the expression(s) for ParameterExpressions and set them to the requested values
 */
void expression_set_parameters(const std::shared_ptr<AbstractExpression>& expression,
                               const std::unordered_map<ParameterID, AllTypeVariant>& parameters);
void expressions_set_parameters(const std::vector<std::shared_ptr<AbstractExpression>>& expressions,
                                const std::unordered_map<ParameterID, AllTypeVariant>& parameters);

/**
 * Traverse the expression(s) for subqueries and set the transaction context in them
 */
void expression_set_transaction_context(const std::shared_ptr<AbstractExpression>& expression,
                                        const std::weak_ptr<TransactionContext>& transaction_context);
void expressions_set_transaction_context(const std::vector<std::shared_ptr<AbstractExpression>>& expressions,
                                         const std::weak_ptr<TransactionContext>& transaction_context);

bool expression_contains_placeholder(const std::shared_ptr<AbstractExpression>& expression);
bool expression_contains_correlated_parameter(const std::shared_ptr<AbstractExpression>& expression);

/**
 * @return  The value of a CorrelatedParameterExpression or ValueExpression
 *          std::nullopt for other expression types
 */
std::optional<AllTypeVariant> expression_get_value_or_parameter(const AbstractExpression& expression);

/**
 * @returns a vector of PQPSubqueryExpressions consisting of @param expression or a subset of its arguments.
 *          If @param expression is not of type PQPSubqueryExpression, this function will search its arguments for
 *          PQPSubqueryExpressions. Note, however, that it will not search for nested PQPSubqueryExpressions.
 */
std::vector<std::shared_ptr<PQPSubqueryExpression>> find_pqp_subquery_expressions(
    const std::shared_ptr<AbstractExpression>& expression);

}  // namespace opossum
