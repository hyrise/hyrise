#include "pqp_reduce_expression.hpp"

#include <cstddef>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/container_hash/hash.hpp>

#include "all_type_variant.hpp"
#include "expression/abstract_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "operators/abstract_operator.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

PQPReduceExpression::PQPReduceExpression(const ColumnID init_column_id,
                                         const std::shared_ptr<AbstractOperator>& reducer, const DataType data_type)
    : AbstractExpression(ExpressionType::PQPReduce, {}),
      column_id{init_column_id},
      _reducer{reducer},
      _data_type{data_type} {}

std::shared_ptr<AbstractExpression> PQPReduceExpression::_on_deep_copy(
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const {
  return std::make_shared<PQPReduceExpression>(column_id, reducer()->deep_copy(copied_ops), _data_type);
}

std::string PQPReduceExpression::description(const DescriptionMode /*mode*/) const {
  auto stream = std::stringstream{};
  stream << "Reduce Column #" << column_id << " (" << reducer() << ")";
  return stream.str();
}

DataType PQPReduceExpression::data_type() const {
  return _data_type;
}

std::shared_ptr<AbstractOperator> PQPReduceExpression::reducer() const {
  auto reducer = _reducer.lock();
  Assert(reducer, "Reducer expired. PQP is invalid.");
  return reducer;
}

bool PQPReduceExpression::_shallow_equals(const AbstractExpression& expression) const {
  // For deep copies of this expression, the PQP is recreated. It is not only difficult to correctly compare PQPs, but
  // it also has unclear semantics. Is an executed PQP equal to a non-executed one? How about two executed ones?
  // As such, we only report equality if two PQPReduceExpression refer to the very same PQP, meaning that a deep copy
  // is not equal to its source. If this ever becomes an issue, it should be easy to spot.
  DebugAssert(dynamic_cast<const PQPReduceExpression*>(&expression),
              "Different expression type should have been caught by AbstractExpression::operator==");
  const auto& other = static_cast<const PQPReduceExpression&>(expression);
  return column_id == other.column_id && reducer() == other.reducer();
}

size_t PQPReduceExpression::_shallow_hash() const {
  auto hash = size_t{0};
  boost::hash_combine(hash, column_id);
  boost::hash_combine(hash, _data_type);
  boost::hash_combine(hash, reducer()->type());  // TODO(anyone): Not a full hash. Implement and use a hash of/on PQPs?
  return hash;
}

bool PQPReduceExpression::_on_is_nullable_on_lqp(const AbstractLQPNode& /*lqp*/) const {
  Fail("Nullability 'on lqp' should never be queried from a PQPSelect.");
}

}  // namespace hyrise
