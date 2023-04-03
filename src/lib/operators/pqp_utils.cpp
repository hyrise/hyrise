#include "pqp_utils.hpp"


namespace hyrise {


std::vector<std::shared_ptr<AbstractOperator>> pqp_find_operators_by_type(const std::shared_ptr<AbstractOperator>& pqp,
                                                                          const OperatorType type) {
  auto operators = std::vector<std::shared_ptr<AbstractOperator>>{};
  visit_pqp(pqp, [&](const auto& op) {
    if (op->type() == type) {
      operators.emplace_back(op);
    }
    return PQPVisitation::VisitInputs;
  });

  return operators;
}

}  // namespace opossum
