#include "transaction_context.hpp"

#include <memory>
#include <stdexcept>

#include "commit_context.hpp"
#include "transaction_manager.hpp"
#include "operators/abstract_read_write_operator.hpp"

#include "utils/assert.hpp"

namespace opossum {

TransactionContext::TransactionContext(const TransactionID transaction_id, const CommitID last_commit_id)
    : _transaction_id{transaction_id},
      _last_commit_id{last_commit_id},
      _phase{TransactionPhase::Active},
      _num_active_operators{0} {}

TransactionID TransactionContext::transaction_id() const { return _transaction_id; }
CommitID TransactionContext::last_commit_id() const { return _last_commit_id; }

CommitID TransactionContext::commit_id() const {
  Assert((_commit_context != nullptr), "TransactionContext cid only available after commit context has been created.");

  return _commit_context->commit_id();
}

TransactionPhase TransactionContext::phase() const { return _phase; }

std::shared_ptr<CommitContext> TransactionContext::commit_context() { return _commit_context; }

bool TransactionContext::mark_as_failed() {
  const auto from_phase = TransactionPhase::Active;
  const auto to_phase = TransactionPhase::Failed;
  return _transition(from_phase, to_phase);
}

bool TransactionContext::mark_as_rolled_back() {
  const auto from_phase = TransactionPhase::Failed;
  const auto to_phase = TransactionPhase::RolledBack;
  const auto success = _transition(from_phase, to_phase);

  if (!success) return false;

  DebugAssert(([this]() {
    for (const auto op : _rw_operators) {
      if (op->state() != ReadWriteOperatorState::RolledBack) return false;
    }
    return true;
  }()), "All read/write operators need to have been rolled back.");

  return true;
}

bool TransactionContext::prepare_commit() {
  const auto from_phase = TransactionPhase::Active;
  const auto to_phase = TransactionPhase::Committing;
  const auto success = _transition(from_phase, to_phase);

  if (!success) return false;

  Assert(_num_active_operators == 0u, "All operators within this transaction need to be finished.");

  DebugAssert(([this]() {
    for (const auto op : _rw_operators) {
      if (op->state() != ReadWriteOperatorState::Executed) return false;
    }
    return true;
  }()), "All read/write operators need to be in state Executed (especially not Failed).");

  _commit_context = TransactionManager::get()._new_commit_context();
  return true;
}

bool TransactionContext::commit(std::function<void(TransactionID)> callback) {
  const auto from_phase = TransactionPhase::Committing;
  const auto to_phase = TransactionPhase::Pending;
  const auto success = _transition(from_phase, to_phase);

  if (!success) return false;

  DebugAssert(([this]() {
    for (const auto op : _rw_operators) {
      if (op->state() != ReadWriteOperatorState::Committed) return false;
    }
    return true;
  }()), "All read/write operators need to have been committed.");


  auto context_weak_ptr = std::weak_ptr<TransactionContext>{this->shared_from_this()};
  _commit_context->make_pending(_transaction_id, [context_weak_ptr, callback](auto transaction_id) {
    // If the transaction context still exists, set its phase to Committed.
    if (auto context_ptr = context_weak_ptr.lock()) {
      context_ptr->_transition(TransactionPhase::Pending, TransactionPhase::Committed);
    }

    if (callback) callback(transaction_id);
  });

  TransactionManager::get()._increment_last_commit_id(_commit_context);
  return true;
}

void TransactionContext::on_operator_started() { ++_num_active_operators; }

void TransactionContext::on_operator_finished() {
  DebugAssert((_num_active_operators > 0), "Bug detected");
  const auto num_before = _num_active_operators--;

  if (num_before == 1) {
    _active_operators_cv.notify_all();
  }
}

void TransactionContext::commit_operators() {
  Assert(_num_active_operators == 0u, "All operators within this transaction need to be finished.");

  for (const auto op : _rw_operators) {
    op->commit_records(commit_id());
  }
}

void TransactionContext::rollback_operators() {
  Assert(_num_active_operators == 0u, "All operators within this transaction need to be finished.");

  for (const auto op : _rw_operators) {
    op->rollback_records();
  }
}

void TransactionContext::wait_for_active_operators_to_finish() const {
  std::unique_lock<std::mutex> lock(_active_operators_mutex);
  if (_num_active_operators == 0) return;
  _active_operators_cv.wait(lock, [&] { return _num_active_operators != 0; });
}

bool TransactionContext::_transition(TransactionPhase from_phase, TransactionPhase to_phase) {
  auto expected = from_phase;
  const auto success = _phase.compare_exchange_strong(expected, to_phase);

  if (success) {
    return true;
  } else {
    Assert(expected == to_phase, "Invalid phase transition detected.");
    // Phase change didn’t succeed because it was already in the target phase
    return false;
  }
}

}  // namespace opossum
