#include "abstract_read_write_operator.hpp"

namespace opossum {

AbstractReadWriteOperator::AbstractReadWriteOperator(const std::shared_ptr<const AbstractOperator> left,
                                                     const std::shared_ptr<const AbstractOperator> right)
    : AbstractOperator(left, right), _state{ReadWriteOperatorState::Pending} {}

std::shared_ptr<AbstractOperator> AbstractReadWriteOperator::recreate(const std::vector<AllParameterVariant>& args) const {
  Fail("Operator " + this->name() + " does not implement recreation.");
  return {};
}

void AbstractReadWriteOperator::execute() {
  Assert(static_cast<bool>(_transaction_context),
         "AbstractReadWriteOperator::execute() should never be called without having set the transaction context.");

  Assert(_state == ReadWriteOperatorState::Pending, "Operator needs to have state Pending in order to be executed.");
  AbstractOperator::execute();

  if (_state == ReadWriteOperatorState::Failed)
    return;

  _state = ReadWriteOperatorState::Executed;
}

void AbstractReadWriteOperator::commit_records(const CommitID cid) {
  Assert(_state == ReadWriteOperatorState::Executed,
         "Operator needs to have state Executed in order to be committed.");

  _on_commit_records(cid);
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

ReadWriteOperatorState AbstractReadWriteOperator::state() const {
  return _state;
}

uint8_t AbstractReadWriteOperator::num_out_tables() const { return 0; };

void AbstractReadWriteOperator::mark_as_failed() {
  Assert(_state == ReadWriteOperatorState::Pending, "Operator can only be marked as failed if pending.");

  _state = ReadWriteOperatorState::Failed;
}

}  // namespace opossum
