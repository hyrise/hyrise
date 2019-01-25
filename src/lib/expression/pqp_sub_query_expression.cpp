#include "pqp_sub_query_expression.hpp"

#include <sstream>

#include "logical_query_plan/abstract_lqp_node.hpp"
#include "operators/abstract_operator.hpp"
#include "utils/assert.hpp"

namespace opossum {

PQPSubQueryExpression::PQPSubQueryExpression(const std::shared_ptr<AbstractOperator>& pqp, const DataType data_type,
                                             const bool nullable,
                                             const std::vector<std::pair<ParameterID, ColumnID>>& parameters)
    : AbstractExpression(ExpressionType::PQPSubQuery, {}),
      pqp(pqp),
      parameters(parameters),
      _data_type_info(std::in_place, data_type, nullable) {}

PQPSubQueryExpression::PQPSubQueryExpression(const std::shared_ptr<AbstractOperator>& pqp, const Parameters& parameters)
    : AbstractExpression(ExpressionType::PQPSubQuery, {}), pqp(pqp), parameters(parameters) {}

std::shared_ptr<AbstractExpression> PQPSubQueryExpression::deep_copy() const {
  if (_data_type_info) {
    return std::make_shared<PQPSubQueryExpression>(pqp->deep_copy(), _data_type_info->data_type,
                                                   _data_type_info->nullable, parameters);
  } else {
    return std::make_shared<PQPSubQueryExpression>(pqp->deep_copy(), parameters);
  }
}

DataType PQPSubQueryExpression::data_type() const {
  Assert(_data_type_info,
         "Can't determine the DataType of this SubQueryExpression, probably because it returns multiple columns");
  return _data_type_info->data_type;
}

bool PQPSubQueryExpression::is_nullable() const {
  Assert(_data_type_info,
         "Can't determine the nullability of this SubQueryExpression, probably because it returns multiple columns");
  return _data_type_info->nullable;
}

bool PQPSubQueryExpression::is_correlated() const { return !parameters.empty(); }

std::string PQPSubQueryExpression::as_column_name() const {
  std::stringstream stream;
  stream << "SUBQUERY (PQP, " << pqp.get() << ")";
  return stream.str();
}

bool PQPSubQueryExpression::_shallow_equals(const AbstractExpression& expression) const {
  Fail("Can't compare PQPSubQueryExpressions");
}

size_t PQPSubQueryExpression::_on_hash() const {
  Fail("PQPSubQueryExpressions can't, shouldn't and shouldn't need to be hashed");
}

PQPSubQueryExpression::DataTypeInfo::DataTypeInfo(const DataType data_type, const bool nullable)
    : data_type(data_type), nullable(nullable) {}

}  // namespace opossum
