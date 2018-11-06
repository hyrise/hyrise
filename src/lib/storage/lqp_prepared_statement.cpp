#include "lqp_prepared_statement.hpp"

#include "logical_query_plan/abstract_lqp_node.hpp"

namespace opossum {

LQPPreparedStatement::LQPPreparedStatement(const std::shared_ptr<AbstractLQPNode>& lqp,
                                           const std::vector<ParameterID>& parameter_ids)
    : lqp(lqp), parameter_ids(parameter_ids) {}

std::shared_ptr<LQPPreparedStatement> LQPPreparedStatement::deep_copy() const {
  const auto lqp_copy = lqp->deep_copy();
  return std::make_shared<LQPPreparedStatement>(lqp_copy, parameter_ids);
}

void LQPPreparedStatement::print(std::ostream& stream) const {
  stream << "ParameterIDs: [";
  for (auto parameter_idx = size_t{0}; parameter_idx < parameter_ids.size(); ++parameter_idx) {
    stream << parameter_ids[parameter_idx];
    if (parameter_idx + 1 < parameter_ids.size()) stream << ", ";
  }
  stream << "]\n";
  lqp->print(stream);
}

bool LQPPreparedStatement::operator==(const LQPPreparedStatement& rhs) const {
  return *lqp == *rhs.lqp && parameter_ids == rhs.parameter_ids;
}

}  // namespace opossum
