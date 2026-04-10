#include "lqp_reduce_expression.hpp"

#include <cstddef>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "all_type_variant.hpp"
#include "expression/abstract_expression.hpp"
#include "expression/lqp_column_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "operators/abstract_operator.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

LQPReduceExpression::LQPReduceExpression(const std::shared_ptr<LQPColumnExpression>& reduced_column,
                                         const std::shared_ptr<AbstractLQPNode>& reducer)
    : AbstractExpression(ExpressionType::LQPReduce, {reduced_column}), _reducer{reducer} {}

std::shared_ptr<AbstractExpression> LQPReduceExpression::_on_deep_copy(
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& /*copied_ops*/) const {
  Fail(
      "LQPReduceExpression should not be copied to avoid copying the linked subplan. See "
      "`map_prunable_subquery_predicates.hpp`.");
}

std::string LQPReduceExpression::description(const DescriptionMode mode) const {
  auto stream = std::stringstream{};
  stream << "Reduce " << reduced_column()->description(mode) << " (" << reducer() << ")";
  return stream.str();
}

DataType LQPReduceExpression::data_type() const {
  return reduced_column()->data_type();
}

std::shared_ptr<LQPColumnExpression> LQPReduceExpression::reduced_column() const {
  return std::static_pointer_cast<LQPColumnExpression>(arguments[0]);
}

std::shared_ptr<AbstractLQPNode> LQPReduceExpression::reducer() const {
  auto reducer = _reducer.lock();
  Assert(reducer, "Reducer expired. LQP is invalid.");
  return reducer;
}

bool LQPReduceExpression::_shallow_equals(const AbstractExpression& expression) const {
  DebugAssert(dynamic_cast<const LQPReduceExpression*>(&expression),
              "Different expression type should have been caught by AbstractExpression::operator==");
  const auto& other = static_cast<const LQPReduceExpression&>(expression);
  return *reducer() == *other.reducer();
}

bool LQPReduceExpression::_on_is_nullable_on_lqp(const AbstractLQPNode& /*lqp*/) const {
  Fail("LQPReduceExpression should not appear in an LQP node's output expressions.");
}

}  // namespace hyrise
