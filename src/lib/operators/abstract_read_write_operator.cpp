#include "abstract_read_write_operator.hpp"

#include <memory>
#include <vector>

namespace opossum {

std::shared_ptr<AbstractOperator> AbstractReadWriteOperator::recreate(
    const std::vector<AllParameterVariant>& args) const {
  // As of now, we only support caching (and thus, recreation) for SELECTs.
  // There should be no conceptual problem with R/W though.
  Fail("ReadWrite operators (here: " + name() + ") can not implement recreation.");
  return {};
}

void AbstractReadWriteOperator::execute() {
  auto transaction_context = _transaction_context.lock();
  DebugAssert(static_cast<bool>(transaction_context), "Probably a bug.");

  transaction_context->on_operator_started();
  _output = _on_execute(transaction_context);
  transaction_context->on_operator_finished();
}

void AbstractReadWriteOperator::commit(opossum::CommitID cid) {
  commit_records(cid);
  finish_commit();
}

bool AbstractReadWriteOperator::execute_failed() const { return _execute_failed; }

}  // namespace opossum
