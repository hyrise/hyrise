#include "lqp_select_expression.hpp"

#include <sstream>

#include "boost/functional/hash.hpp"

#include "expression_utils.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "utils/assert.hpp"


namespace opossum {

LQPSelectExpression::LQPSelectExpression(const std::shared_ptr<AbstractLQPNode>& lqp,
                                         const std::vector<std::shared_ptr<AbstractExpression>>& referenced_external_expressions):
AbstractSelectExpression(referenced_external_expressions), lqp(lqp) {

}

std::shared_ptr<AbstractExpression> LQPSelectExpression::deep_copy() const {
  return std::make_shared<LQPSelectExpression>(lqp->deep_copy(), expressions_copy(referenced_external_expressions()));
}

std::string LQPSelectExpression::as_column_name() const {
  std::stringstream stream;
  lqp->print(stream);
  return stream.str();
}

DataType LQPSelectExpression::data_type() const {
  Assert(lqp->output_column_expressions().size() == 1, "Subselects must return one column");
  return lqp->output_column_expressions()[0]->data_type();
}

const std::vector<std::shared_ptr<AbstractExpression>>& LQPSelectExpression::referenced_external_expressions() const {
  return arguments;
}

bool LQPSelectExpression::_shallow_equals(const AbstractExpression& expression) const {
  return !lqp_find_subplan_mismatch(lqp, static_cast<const LQPSelectExpression&>(expression).lqp);
}

size_t LQPSelectExpression::_on_hash() const {
  return 0;  // TODO(moritz)
}

}  // namespace opossum
