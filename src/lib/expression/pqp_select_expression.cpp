#include "pqp_select_expression.hpp"

#include <sstream>

#include "logical_query_plan/abstract_lqp_node.hpp"
#include "operators/abstract_operator.hpp"
#include "utils/assert.hpp"

namespace opossum {

PQPSelectExpression::PQPSelectExpression(const std::shared_ptr<AbstractOperator>& pqp, const DataType data_type,
                                         const bool nullable,
                                         const std::vector<std::pair<ParameterID, ColumnID>>& parameters)
    : AbstractExpression(ExpressionType::PQPSelect, {}),
      pqp(pqp),
      parameters(parameters),
      _data_type_info(std::in_place, data_type, nullable) {}

PQPSelectExpression::PQPSelectExpression(const std::shared_ptr<AbstractOperator>& pqp, const Parameters& parameters)
    : AbstractExpression(ExpressionType::PQPSelect, {}), pqp(pqp), parameters(parameters) {}

std::shared_ptr<AbstractExpression> PQPSelectExpression::deep_copy() const {
  if (_data_type_info) {
    return std::make_shared<PQPSelectExpression>(pqp->deep_copy(), _data_type_info->data_type,
                                                 _data_type_info->nullable, parameters);
  } else {
    return std::make_shared<PQPSelectExpression>(pqp->deep_copy(), parameters);
  }
}

DataType PQPSelectExpression::data_type() const {
  Assert(_data_type_info,
         "Can't determine the DataType of this SelectExpression, probably because it returns multiple columns");
  return _data_type_info->data_type;
}

bool PQPSelectExpression::is_nullable() const {
  Assert(_data_type_info,
         "Can't determine the nullability of this SelectExpression, probably because it returns multiple columns");
  return _data_type_info->nullable;
}

bool PQPSelectExpression::is_correlated() const { return !parameters.empty(); }

std::string PQPSelectExpression::as_column_name() const {
  std::stringstream stream;
  stream << "SUBSELECT (PQP, " << pqp.get() << ")";
  return stream.str();
}

bool PQPSelectExpression::_shallow_equals(const AbstractExpression& expression) const {
  Fail("Can't compare PQPSelectExpressions");
}

size_t PQPSelectExpression::_on_hash() const {
  Fail("PQPSelectExpressions can't, shouldn't and shouldn't need to be hashed");
}

PQPSelectExpression::DataTypeInfo::DataTypeInfo(const DataType data_type, const bool nullable)
    : data_type(data_type), nullable(nullable) {}

}  // namespace opossum
