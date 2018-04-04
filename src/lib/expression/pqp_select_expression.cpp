#include "lqp_select_expression.hpp"

#include <sstream>

#include "logical_query_plan/abstract_lqp_node.hpp"
#include "utils/assert.hpp"

namespace opossum {

PQPSelectExpression::PQPSelectExpression(const std::shared_ptr<AbstractOperator>& pqp): AbstractExpression(ExpressionType::Select, {}), pqp(pqp) {

}

std::shared_ptr<AbstractExpression> PQPSelectExpression::deep_copy() const {
  return std::make_shared<PQPSelectExpression>(pqp->recreate());
}

std::string PQPSelectExpression::description() const {
  std::stringstream stream;

  Fail("Todo");
  return stream.str();
}

bool PQPSelectExpression::_shallow_equals(const AbstractExpression& expression) const {
  Fail("Can't compare PQPSelectExpression");
}

}  // namespace opossum
