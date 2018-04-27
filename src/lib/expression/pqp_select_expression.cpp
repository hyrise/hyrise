#include "pqp_select_expression.hpp"

#include <sstream>

#include "operators/abstract_operator.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "utils/assert.hpp"

namespace opossum {

PQPSelectExpression::PQPSelectExpression(const std::shared_ptr<AbstractOperator>& pqp,
                                         const DataType data_type_,
                                         const std::vector<PQPSelectParameter>& parameters):
  AbstractExpression(ExpressionType::Select, {}), pqp(pqp), parameters(parameters), data_type_(data_type_) {

}

bool PQPSelectExpression::requires_calculation() const {
  return true;
}

std::shared_ptr<AbstractExpression> PQPSelectExpression::deep_copy() const {
  return std::make_shared<PQPSelectExpression>(pqp->recreate(), data_type_, parameters);
}

ExpressionDataTypeVariant PQPSelectExpression::data_type() const {
  return data_type_;
}

std::string PQPSelectExpression::as_column_name() const {
  std::stringstream stream;

  Fail("Todo");
  return stream.str();
}

bool PQPSelectExpression::_shallow_equals(const AbstractExpression& expression) const {
  Fail("Can't compare PQPSelectExpression");
}

size_t PQPSelectExpression::_on_hash() const {
  Fail("PQPSelectExpressions can't, shouldn't and shouldn't need to be hashed");
}

}  // namespace opossum
