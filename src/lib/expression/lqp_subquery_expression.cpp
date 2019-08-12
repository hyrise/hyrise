#include "lqp_subquery_expression.hpp"

#include <sstream>

#include "boost/functional/hash.hpp"

#include "expression_utils.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

LQPSubqueryExpression::LQPSubqueryExpression(
    const std::shared_ptr<AbstractLQPNode>& lqp, const std::vector<ParameterID>& parameter_ids,
    const std::vector<std::shared_ptr<AbstractExpression>>& parameter_expressions)
    : AbstractExpression(ExpressionType::LQPSubquery, parameter_expressions), lqp(lqp), parameter_ids(parameter_ids) {
  Assert(parameter_ids.size() == parameter_expressions.size(),
         "Need exactly as many ParameterIDs as parameter Expressions");
}

size_t LQPSubqueryExpression::parameter_count() const { return parameter_ids.size(); }

std::shared_ptr<AbstractExpression> LQPSubqueryExpression::parameter_expression(const size_t parameter_idx) const {
  Assert(parameter_idx < parameter_count(), "Parameter index out of range");
  return arguments[parameter_idx];
}

std::shared_ptr<AbstractExpression> LQPSubqueryExpression::deep_copy() const {
  const auto lqp_copy = lqp->deep_copy();

  return std::make_shared<LQPSubqueryExpression>(lqp_copy, parameter_ids, expressions_deep_copy(arguments));
}

std::string LQPSubqueryExpression::as_column_name() const {
  std::stringstream stream;
  stream << "SUBQUERY (LQP, " << lqp.get();

  if (!arguments.empty()) {
    stream << ", Parameters: ";
    for (auto parameter_idx = size_t{0}; parameter_idx < arguments.size(); ++parameter_idx) {
      stream << "[" << arguments[parameter_idx]->as_column_name() << ", id=" << parameter_ids[parameter_idx] << "]";
      if (parameter_idx + 1 < arguments.size()) stream << ", ";
    }
  }

  stream << ")";

  return stream.str();
}

DataType LQPSubqueryExpression::data_type() const {
  Assert(lqp->column_expressions().size() == 1,
         "Can only determine the DataType of SubqueryExpressions that return exactly one column");
  return lqp->column_expressions()[0]->data_type();
}

bool LQPSubqueryExpression::_on_is_nullable_on_lqp(const AbstractLQPNode&) const {
  Assert(lqp->column_expressions().size() == 1,
         "Can only determine the nullability of SelectExpressions that return exactly one column");
  return lqp->is_column_nullable(ColumnID{0});
}

bool LQPSubqueryExpression::is_correlated() const { return !arguments.empty(); }

bool LQPSubqueryExpression::_shallow_equals(const AbstractExpression& expression) const {
  DebugAssert(dynamic_cast<const LQPSubqueryExpression*>(&expression),
              "Different expression type should have been caught by AbstractExpression::operator==");
  const auto& subquery_expression = static_cast<const LQPSubqueryExpression&>(expression);
  return *lqp == *subquery_expression.lqp && parameter_ids == subquery_expression.parameter_ids;
}

size_t LQPSubqueryExpression::_shallow_hash() const {
  // Return 0, thus forcing a hash collision for LQPSubqueryExpressions and triggering a full equality check.
  // TODO(moritz) LQP hashing will be introduced with the JoinOrdering optimizer, until then we live with these
  //              collisions
  return AbstractExpression::_shallow_hash();
}

}  // namespace opossum
