#pragma once

#include <functional>
#include <memory>
#include <queue>
#include <unordered_map>
#include <vector>

#include "abstract_expression.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "parameter_expression.hpp"

namespace opossum {

class AbstractLQPNode;
class LQPColumnExpression;
class TransactionContext;

/**
 * Utility to check whether two vectors of Expressions are equal according to AbstractExpression::operator==()
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
 * Utility to AbstractExpression::deep_copy() a vector of expressions
 */
std::vector<std::shared_ptr<AbstractExpression>> expressions_deep_copy(
    const std::vector<std::shared_ptr<AbstractExpression>>& expressions);

/**
 * Utility to AbstractExpression::deep_copy() a vector of expressions while adjusting column references in
 * LQPColumnExpressions according to the node_mapping
 */
std::vector<std::shared_ptr<AbstractExpression>> expressions_copy_and_adapt_to_different_lqp(
    const std::vector<std::shared_ptr<AbstractExpression>>& expressions, const LQPNodeMapping& node_mapping);

/**
 * Utility to AbstractExpression::deep_copy() a single expression while adjusting column references in
 * LQPColumnExpressions according to the node_mapping
 */
std::shared_ptr<AbstractExpression> expression_copy_and_adapt_to_different_lqp(const AbstractExpression& expression,
                                                                               const LQPNodeMapping& node_mapping);

/**
 * Makes all ColumnExpressions points to their equivalent in a copied LQP
 */
void expression_adapt_to_different_lqp(std::shared_ptr<AbstractExpression>& expression,
                                       const LQPNodeMapping& node_mapping);

std::shared_ptr<LQPColumnExpression> expression_adapt_to_different_lqp(const LQPColumnExpression& lqp_column_expression,
                                                                       const LQPNodeMapping& node_mapping);

/**
 * Create a comma separated string with the AbstractExpression::as_column_name() of each expression
 */
std::string expression_column_names(const std::vector<std::shared_ptr<AbstractExpression>>& expressions);

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
 */
DataType expression_common_type(const DataType lhs, const DataType rhs);

/**
 * @return Checks whether the expression can be evaluated by the ExpressionEvaluator on top of a specified LQP (i.e.,
 *         all required LQPColumnExpressions are available from this LQP).
 *         To check if an expression is available in a form ready to be used by a scan/join,
 *         use `Operator*Predicate::from_expression(...) != nullptr`.
 */
bool expression_evaluable_on_lqp(const std::shared_ptr<AbstractExpression>& expression, const AbstractLQPNode& lqp);

/**
 * Convert "a AND b AND c" to [a,b,c] where a,b,c can be arbitrarily complex expressions
 */
std::vector<std::shared_ptr<AbstractExpression>> expression_flatten_conjunction(
    const std::shared_ptr<AbstractExpression>& expression);

/**
 * Traverse the expression(s) for ParameterExpressions and set them to the requested values
 */
void expression_set_parameters(const std::shared_ptr<AbstractExpression>& expression,
                               const std::unordered_map<ParameterID, AllTypeVariant>& parameters);
void expressions_set_parameters(const std::vector<std::shared_ptr<AbstractExpression>>& expressions,
                                const std::unordered_map<ParameterID, AllTypeVariant>& parameters);

/**
 * Traverse the expression(s) for subselects and set the transaction context in them
 */
void expression_set_transaction_context(const std::shared_ptr<AbstractExpression>& expression,
                                        const std::weak_ptr<TransactionContext>& transaction_context);
void expressions_set_transaction_context(const std::vector<std::shared_ptr<AbstractExpression>>& expressions,
                                         const std::weak_ptr<TransactionContext>& transaction_context);

bool expression_contains_placeholders(const std::shared_ptr<AbstractExpression>& expression);

}  // namespace opossum
