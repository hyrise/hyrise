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

std::string PQPSubqueryExpression::description(const DescriptionMode mode) const {
  std::stringstream stream;
  stream << "SUBQUERY (PQP, " << pqp.get() << ")";
  return stream.str();
}

bool PQPSubqueryExpression::_shallow_equals(const AbstractExpression& expression) const {
  // For deep copies of this expression, the PQP is recreated. It is not only difficult to correctly compare PQPs, but
  // it also has unclear semantics. Is an executed PQP equal to a non-executed one? How about two executed ones?
  // As such, we only report equality if two PQPSubqueryExpression refer to the very same PQP, meaning that a deep copy
  // is not equal to its source. If this ever becomes an issue, it should be easy to spot.
  DebugAssert(dynamic_cast<const PQPSubqueryExpression*>(&expression),
              "Different expression type should have been caught by AbstractExpression::operator==");
  const auto& other = static_cast<const PQPSubqueryExpression&>(expression);
  return pqp == other.pqp && parameters == parameters;
}

size_t PQPSubqueryExpression::_shallow_hash() const {
  size_t hash{0};
  boost::hash_combine(hash, boost::hash_range(parameters.cbegin(), parameters.cend()));
  boost::hash_combine(hash, pqp->type());  // TODO(anyone): Not a full hash. Implement and use a hash of/on PQPs?
  return hash;
}

bool PQPSubqueryExpression::_on_is_nullable_on_lqp(const AbstractLQPNode& lqp) const {
  Fail("Nullability 'on lqp' should never be queried from a PQPSelect");
}

PQPSubqueryExpression::DataTypeInfo::DataTypeInfo(const DataType data_type, const bool nullable)
    : data_type(data_type), nullable(nullable) {}

}  // namespace opossum
