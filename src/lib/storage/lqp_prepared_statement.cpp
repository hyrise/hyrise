#include "lqp_prepared_statement.hpp"

#include "logical_query_plan/abstract_lqp_node.hpp"

namespace opossum {

LQPPreparedStatement::LQPPreparedStatement(const std::shared_ptr<AbstractLQPNode>& lqp, const std::vector<ParameterID>& parameter_ids):
  lqp(lqp), parameter_ids(parameter_ids) { }

std::shared_ptr<LQPPreparedStatement> LQPPreparedStatement::deep_copy() const {
  const auto lqp_copy = lqp->deep_copy();
  return std::make_shared<LQPPreparedStatement>(lqp_copy, parameter_ids);
}

}  // namespace opossum
