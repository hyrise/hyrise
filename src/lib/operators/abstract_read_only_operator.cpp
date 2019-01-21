#include "abstract_read_only_operator.hpp"

#include <memory>

#include "storage/table.hpp"

namespace opossum {

std::shared_ptr<const Table> AbstractReadOnlyOperator::_on_execute(std::shared_ptr<TransactionContext>) {
  DebugAssert(
      !input_left() || !input_right() || input_table_left()->is_validated() == input_table_right()->is_validated(),
      "Either none or both input tables should be validated");

  return _on_execute();
}

}  // namespace opossum
