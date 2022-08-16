#include "abstract_read_only_operator.hpp"

#include <memory>

#include "storage/table.hpp"

namespace hyrise {

std::shared_ptr<const Table> AbstractReadOnlyOperator::_on_execute(std::shared_ptr<TransactionContext> /*context*/) {
  return _on_execute();
}

}  // namespace hyrise
