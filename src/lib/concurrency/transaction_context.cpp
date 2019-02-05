#include "transaction_context.hpp"

#include <future>
#include <memory>
#include <string>

#include "commit_context.hpp"
#include "operators/abstract_read_write_operator.hpp"
#include "storage/constraints/unique_checker.hpp"
#include "transaction_manager.hpp"
#include "utils/assert.hpp"

namespace opossum {

TransactionContext::TransactionContext(const TransactionID transaction_id, const CommitID snapshot_commit_id)
    : _transaction_id{transaction_id},
      _snapshot_commit_id{snapshot_commit_id},
      _phase{TransactionPhase::Active},
      _num_active_operators{0} {}

TransactionContext::~TransactionContext() {
  DebugAssert(([this]() {
                auto an_operator_failed = false;
                for (const auto& op : _rw_operators) {
                  if (op->state() == ReadWriteOperatorState::Failed) {
                    an_operator_failed = true;
                    break;
                  }
                }

                const auto is_rolled_back = _phase == TransactionPhase::RolledBack;
                return (!an_operator_failed || is_rolled_back);
              }()),
              "A registered operator failed but the transaction has not been rolled back. You may also see this "
              "exception if an operator threw an uncaught exception.");

  DebugAssert(([this]() {
                const auto has_registered_operators = !_rw_operators.empty();
                const auto committed_or_rolled_back =
                    _phase == TransactionPhase::Committed || _phase == TransactionPhase::RolledBack;
                return !has_registered_operators || committed_or_rolled_back;
              }()),
              "Has registered operators but has neither been committed nor rolled back.");
}

TransactionID TransactionContext::transaction_id() const { return _transaction_id; }
CommitID TransactionContext::snapshot_commit_id() const { return _snapshot_commit_id; }

CommitID TransactionContext::commit_id() const {
  Assert((_commit_context != nullptr), "TransactionContext cid only available after commit context has been created.");

  return _commit_context->commit_id();
}

TransactionPhase TransactionContext::phase() const { return _phase; }

bool TransactionContext::aborted() const {
  const auto phase = _phase.load();
  return (phase == TransactionPhase::Aborted) || (phase == TransactionPhase::RolledBack);
}

bool TransactionContext::rollback() {
  const auto success = _abort();

  if (!success) return false;

  for (const auto& op : _rw_operators) {
    op->rollback_records();
  }

  _mark_as_rolled_back();

  return true;
}

bool TransactionContext::commit_async(const std::function<void(TransactionID)>& callback) {
  const auto success = _prepare_commit();

  if (!success) return false;

  // Check all _rw_operators potential violations of unique constraints.
  // If the constraint check fails, set the commit as failed.
  for (const auto& op : _rw_operators) {
    const auto& type = op->type();
    // TOOD(all): Remove as soon as the transaction context phase model got refactored
    if (type == OperatorType::Insert &&
        !all_constraints_valid_for(op->table_name(), _commit_context->commit_id(), _transaction_id)) {
      _transition(TransactionPhase::Committing, TransactionPhase::Active, TransactionPhase::RolledBack);
      return false;
    }
  }

  for (const auto& op : _rw_operators) {
    op->commit_records(commit_id());
  }

  _mark_as_pending_and_try_commit(callback);

  return true;
}

bool TransactionContext::commit() {
  auto committed = std::promise<void>{};
  const auto committed_future = committed.get_future();
  const auto callback = [&committed](TransactionID) { committed.set_value(); };

  const auto success = commit_async(callback);
  if (!success) return false;

  committed_future.wait();
  return true;
}

bool TransactionContext::_abort() {
  const auto from_phase = TransactionPhase::Active;
  const auto to_phase = TransactionPhase::Aborted;
  const auto end_phase = TransactionPhase::RolledBack;
  auto success = _transition(from_phase, to_phase, end_phase);

  if (!success) return false;

  _wait_for_active_operators_to_finish();
  return true;
}

void TransactionContext::_mark_as_rolled_back() {
  DebugAssert(([this]() {
                for (const auto& op : _rw_operators) {
                  if (op->state() != ReadWriteOperatorState::RolledBack) return false;
                }
                return true;
              }()),
              "All read/write operators need to have been rolled back.");

  _phase = TransactionPhase::RolledBack;
}

bool TransactionContext::_prepare_commit() {
  const auto from_phase = TransactionPhase::Active;
  const auto to_phase = TransactionPhase::Committing;
  const auto end_phase = TransactionPhase::Committed;
  const auto success = _transition(from_phase, to_phase, end_phase);

  if (!success) return false;

  DebugAssert(([this]() {
                for (const auto& op : _rw_operators) {
                  if (op->state() != ReadWriteOperatorState::Executed) return false;
                }
                return true;
              }()),
              "All read/write operators need to be in state Executed (especially not Failed).");

  _wait_for_active_operators_to_finish();

  _commit_context = TransactionManager::get()._new_commit_context();
  return true;
}

void TransactionContext::_mark_as_pending_and_try_commit(std::function<void(TransactionID)> callback) {
  DebugAssert(([this]() {
                for (const auto& op : _rw_operators) {
                  if (op->state() != ReadWriteOperatorState::Committed) return false;
                }
                return true;
              }()),
              "All read/write operators need to have been committed.");

  auto context_weak_ptr = std::weak_ptr<TransactionContext>{this->shared_from_this()};
  _commit_context->make_pending(_transaction_id, [context_weak_ptr, callback](auto transaction_id) {
    // If the transaction context still exists, set its phase to Committed.
    if (auto context_ptr = context_weak_ptr.lock()) {
      context_ptr->_phase = TransactionPhase::Committed;
    }

    if (callback) callback(transaction_id);
  });

  TransactionManager::get()._try_increment_last_commit_id(_commit_context);
}

void TransactionContext::on_operator_started() { ++_num_active_operators; }

void TransactionContext::on_operator_finished() {
  DebugAssert((_num_active_operators > 0), "Bug detected");
  const auto num_before = _num_active_operators--;

  if (num_before == 1) {
    _active_operators_cv.notify_all();
  }
}

void TransactionContext::_wait_for_active_operators_to_finish() const {
  std::unique_lock<std::mutex> lock(_active_operators_mutex);
  if (_num_active_operators == 0) return;
  _active_operators_cv.wait(lock, [&] { return _num_active_operators != 0; });
}

bool TransactionContext::_transition(TransactionPhase from_phase, TransactionPhase to_phase,
                                     TransactionPhase end_phase) {
  auto expected = from_phase;
  const auto success = _phase.compare_exchange_strong(expected, to_phase);

  if (success) {
    return true;
  } else {
    Assert((expected == to_phase) || (expected == end_phase), "Invalid phase transition detected.");
    return false;
  }
}

}  // namespace opossum
