#include "commit_transaction_task.hpp"

#include "concurrency/transaction_context.hpp"

namespace opossum {

void CommitTransactionTask::_on_execute() {
  try {
    _transaction->commit();
    _promise.set_value();
  } catch (const std::exception& exception) {
    _promise.set_exception(boost::current_exception());
  }
}

}  // namespace opossum
