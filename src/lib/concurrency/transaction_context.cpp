#include "transaction_context.hpp"

#include <future>
#include <memory>

#include "commit_context.hpp"
#include "hyrise.hpp"
#include "operators/abstract_read_write_operator.hpp"
#include "utils/assert.hpp"

namespace opossum {

TransactionContext::TransactionContext(const TransactionID transaction_id, const CommitID snapshot_commit_id)
    : _transaction_id{transaction_id},
      _snapshot_commit_id{snapshot_commit_id},
      _phase{TransactionPhase::Active},
      _num_active_operators{0} {
  Hyrise::get().transaction_manager._register_transaction(snapshot_commit_id);
}

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
                // Note: When thrown during stack unwinding, this exception might hide previous exceptions. If you are
                // seeing this, either use a debugger and break on exceptions or disable this exception as a trial.
              }()),
              "Has registered operators but has neither been committed nor rolled back.");

  /**
   * Tell the TransactionManager, which keeps track of active snapshot-commit-ids,
   * that this transaction has finished.
   */
  Hyrise::get().transaction_manager._deregister_transaction(_snapshot_commit_id);
}

TransactionID TransactionContext::transaction_id() const { return _transaction_id; }
CommitID TransactionContext::snapshot_commit_id() const { return _snapshot_commit_id; }

CommitID TransactionContext::commit_id() const {
  Assert(_commit_context, "TransactionContext cid only available after commit context has been created.");

  return _commit_context->commit_id();
}

TransactionPhase TransactionContext::phase() const { return _phase; }

bool TransactionContext::aborted() const {
  const auto phase = _phase.load();
  return (phase == TransactionPhase::Aborted) || (phase == TransactionPhase::RolledBack);
}

void TransactionContext::rollback() {
  _abort();

  for (const auto& op : _rw_operators) {
    op->rollback_records();
  }

  _mark_as_rolled_back();
}

void TransactionContext::commit_async(const std::function<void(TransactionID)>& callback) {
  _prepare_commit();

  for (const auto& op : _rw_operators) {
    op->commit_records(commit_id());
  }

  _mark_as_pending_and_try_commit(callback);
}

void TransactionContext::commit() {
  Assert(_phase == TransactionPhase::Active, "TransactionContext must be active to be committed.");

  // No modifications made, nothing to commit, no need to acquire a commit ID
  if (_rw_operators.empty()) {
    _transition(TransactionPhase::Active, TransactionPhase::Committed);
    return;
  }

  auto committed = std::promise<void>{};
  const auto committed_future = committed.get_future();
  const auto callback = [&committed](TransactionID) { committed.set_value(); };

  commit_async(callback);

  committed_future.wait();
}

void TransactionContext::_abort() {
  _transition(TransactionPhase::Active, TransactionPhase::Aborted);

  _wait_for_active_operators_to_finish();
}

void TransactionContext::_mark_as_rolled_back() {
  DebugAssert(([this]() {
                for (const auto& op : _rw_operators) {
                  if (op->state() != ReadWriteOperatorState::RolledBack) return false;
                }
                return true;
              }()),
              "All read/write operators need to have been rolled back.");

  _transition(TransactionPhase::Aborted, TransactionPhase::RolledBack);
}

void TransactionContext::_prepare_commit() {
  DebugAssert(([this]() {
                for (const auto& op : _rw_operators) {
                  if (op->state() != ReadWriteOperatorState::Executed) return false;
                }
                return true;
              }()),
              "All read/write operators need to be in state Executed (especially not Failed).");

  _transition(TransactionPhase::Active, TransactionPhase::Committing);

  _wait_for_active_operators_to_finish();

  _commit_context = Hyrise::get().transaction_manager._new_commit_context();
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
      context_ptr->_transition(TransactionPhase::Committing, TransactionPhase::Committed);
    }

    if (callback) callback(transaction_id);
  });

  Hyrise::get().transaction_manager._try_increment_last_commit_id(_commit_context);
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

void TransactionContext::_transition(TransactionPhase from_phase, TransactionPhase to_phase) {
  const auto success = _phase.compare_exchange_strong(from_phase, to_phase);
  Assert(success, "Illegal phase transition detected.");
}

}  // namespace opossum
