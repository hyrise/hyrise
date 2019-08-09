#include "pqp_subquery_expression.hpp"

#include <sstream>

#include "logical_query_plan/abstract_lqp_node.hpp"
#include "operators/abstract_operator.hpp"
#include "utils/assert.hpp"

namespace opossum {

PQPSubqueryExpression::PQPSubqueryExpression(const std::shared_ptr<AbstractOperator>& pqp, const DataType data_type,
                                             const bool nullable,
                                             const std::vector<std::pair<ParameterID, ColumnID>>& parameters)
    : AbstractExpression(ExpressionType::PQPSubquery, {}),
      pqp(pqp),
      parameters(parameters),
      _data_type_info(std::in_place, data_type, nullable) {}

PQPSubqueryExpression::PQPSubqueryExpression(const std::shared_ptr<AbstractOperator>& pqp, const Parameters& parameters)
    : AbstractExpression(ExpressionType::PQPSubquery, {}), pqp(pqp), parameters(parameters) {}

std::shared_ptr<AbstractExpression> PQPSubqueryExpression::deep_copy() const {
  if (_data_type_info) {
    return std::make_shared<PQPSubqueryExpression>(pqp->deep_copy(), _data_type_info->data_type,
                                                   _data_type_info->nullable, parameters);
  } else {
    return std::make_shared<PQPSubqueryExpression>(pqp->deep_copy(), parameters);
  }
}

DataType PQPSubqueryExpression::data_type() const {
  Assert(_data_type_info,
         "Can't determine the DataType of this SubqueryExpression, probably because it returns multiple columns");
  return _data_type_info->data_type;
}

bool PQPSubqueryExpression::is_correlated() const { return !parameters.empty(); }

std::string PQPSubqueryExpression::as_column_name() const {
  std::stringstream stream;
  stream << "SUBQUERY (PQP, " << pqp.get() << ")";
  return stream.str();
}

bool PQPSubqueryExpression::_shallow_equals(const AbstractExpression& expression) const {
  Fail("Can't compare PQPSubqueryExpressions");
}

size_t PQPSubqueryExpression::_on_hash() const {
  Fail("PQPSubqueryExpressions can't, shouldn't and shouldn't need to be hashed");
}

bool PQPSubqueryExpression::_on_is_nullable_on_lqp(const AbstractLQPNode& lqp) const {
  Fail("Nullability 'on lqp' should never be queried from a PQPSelect");
}

PQPSubqueryExpression::DataTypeInfo::DataTypeInfo(const DataType data_type, const bool nullable)
    : data_type(data_type), nullable(nullable) {}

}  // namespace opossum
