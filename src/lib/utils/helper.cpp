#include "helper.hpp"

namespace opossum {
void set_transaction_context_for_operators(const std::shared_ptr<TransactionContext> t_context,
                                           const std::vector<std::shared_ptr<AbstractOperator>> operators) {
  for (auto& op : operators) {
    op->set_transaction_context(t_context);
  }
}
}