#include "abstract_read_write_operator.hpp"

#include <memory>
#include <vector>

namespace opossum {

AbstractReadWriteOperator::AbstractReadWriteOperator(const std::shared_ptr<const AbstractOperator> left,
                                                     const std::shared_ptr<const AbstractOperator> right)
    : AbstractOperator(left, right), _state{ReadWriteOperatorState::Pending} {}

std::shared_ptr<AbstractOperator> AbstractReadWriteOperator::recreate(
    const std::vector<AllParameterVariant>& args) const {
  // As of now, we only support caching (and thus, recreation) for SELECTs.
  // There should be no conceptual problem with R/W though.
  Fail("ReadWrite operators (here: " + name() + ") can not implement recreation.");
}

void AbstractReadWriteOperator::execute() {
  Assert(static_cast<bool>(transaction_context()),
         "AbstractReadWriteOperator::execute() should never be called without having set the transaction context.");

  Assert(_state == ReadWriteOperatorState::Pending, "Operator needs to have state Pending in order to be executed.");
  AbstractOperator::execute();

  if (_state == ReadWriteOperatorState::Failed) return;

  _state = ReadWriteOperatorState::Executed;
}

void AbstractReadWriteOperator::commit_records(const CommitID commit_id) {
  Assert(_state == ReadWriteOperatorState::Executed, "Operator needs to have state Executed in order to be committed.");

  _on_commit_records(commit_id);
  _finish_commit();

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
