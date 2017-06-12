#include "helper.hpp"

#include "concurrency/transaction_context.hpp"
#include "operators/abstract_operator.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/print.hpp"

namespace opossum {
void set_transaction_context_for_operators(const std::shared_ptr<TransactionContext> t_context,
                                           const std::vector<std::shared_ptr<AbstractOperator>> operators) {
  for (auto& op : operators) {
    op->set_transaction_context(t_context);
  }
}

void print_table(const std::shared_ptr<const Table> table, PrintMode mode, std::ostream& out) {
  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();
  Print(table_wrapper, out, mode).execute();
}

}