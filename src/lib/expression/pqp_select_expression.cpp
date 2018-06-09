#include "pqp_select_expression.hpp"

#include <sstream>

#include "operators/abstract_operator.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "utils/assert.hpp"

namespace opossum {

PQPSelectExpression::PQPSelectExpression(const std::shared_ptr<AbstractOperator>& pqp,
                                         const DataType data_type,
                                         const bool nullable,
                                         const std::vector<std::pair<ParameterID, ColumnID>>& parameters):
  pqp(pqp), parameters(parameters), _data_type(data_type), _nullable(nullable) {

}


std::shared_ptr<AbstractExpression> PQPSelectExpression::deep_copy() const {
  return std::make_shared<PQPSelectExpression>(pqp->recreate(), _data_type, _nullable, parameters);
}

DataType PQPSelectExpression::data_type() const {
  return _data_type;
}

bool PQPSelectExpression::is_nullable() const {
  return _nullable;
}

std::string PQPSelectExpression::as_column_name() const {
  std::stringstream stream;
  stream << "SUBSELECT";
  return stream.str();
}

bool PQPSelectExpression::_shallow_equals(const AbstractExpression& expression) const {
  Fail("Can't compare PQPSelectExpression");
}

size_t PQPSelectExpression::_on_hash() const {
  Fail("PQPSelectExpressions can't, shouldn't and shouldn't need to be hashed");
}

}  // namespace opossum
