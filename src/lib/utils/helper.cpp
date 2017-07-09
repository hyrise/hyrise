#include "helper.hpp"

#include <memory>
#include <vector>

#include "concurrency/transaction_context.hpp"
#include "operators/abstract_operator.hpp"
#include "operators/print.hpp"
#include "operators/table_wrapper.hpp"
#include "scheduler/abstract_scheduler.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/operator_task.hpp"

namespace opossum {

void print_table(const std::shared_ptr<const Table> table, uint32_t flags, std::ostream& out) {
  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();
  Print(table_wrapper, out, flags).execute();
}

}  // namespace opossum
