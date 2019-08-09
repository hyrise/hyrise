#include "abstract_read_write_operator.hpp"

#include <memory>
#include <vector>

namespace opossum {

AbstractReadWriteOperator::AbstractReadWriteOperator(const OperatorType type,
                                                     const std::shared_ptr<const AbstractOperator>& left,
                                                     const std::shared_ptr<const AbstractOperator>& right)
    : AbstractOperator(type, left, right), _state{ReadWriteOperatorState::Pending} {}

void AbstractReadWriteOperator::execute() {
  DebugAssert(!_output, "Operator has already been executed");

  Assert(static_cast<bool>(transaction_context()),
         "AbstractReadWriteOperator::execute() should never be called without having set the transaction context.");

  DebugAssert(transaction_context()->phase() == TransactionPhase::Active, "Transaction is not active anymore.");

  Assert(_state == ReadWriteOperatorState::Pending, "Operator needs to have state Pending in order to be executed.");

  try {
    AbstractOperator::execute();
  } catch (...) {
    // No matter what goes wrong, we need to mark the operators as failed. Otherwise, when the transaction context
    // gets destroyed, it will cause another exception that hides the one that caused the actual error. We are NOT
    // trying to handle the exception here - just making sure that we are not misled when we debug things.
    _mark_as_failed();
    throw;
  }

  if (_state == ReadWriteOperatorState::Failed) return;

  _state = ReadWriteOperatorState::Executed;
}

void AbstractReadWriteOperator::commit_records(const CommitID commit_id) {
  Assert(_state == ReadWriteOperatorState::Executed, "Operator needs to have state Executed in order to be committed.");

  _on_commit_records(commit_id);
  _state = ReadWriteOperatorState::Committed;
}

void AbstractReadWriteOperator::rollback_records() {
  Assert(_state == ReadWriteOperatorState::Failed || _state == ReadWriteOperatorState::Executed,
         "Operator needs to have state Failed or Executed in order to be rolled back.");

  _on_rollback_records();

  _state = ReadWriteOperatorState::RolledBack;
}

bool AbstractReadWriteOperator::execute_failed() const {
  return _state == ReadWriteOperatorState::Failed || _state == ReadWriteOperatorState::RolledBack;
}

ReadWriteOperatorState AbstractReadWriteOperator::state() const { return _state; }

void AbstractReadWriteOperator::_mark_as_failed() {
  Assert(_state == ReadWriteOperatorState::Pending, "Operator can only be marked as failed if pending.");

  _state = ReadWriteOperatorState::Failed;
}

}  // namespace opossum
