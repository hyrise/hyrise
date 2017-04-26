#include "transaction_context.hpp"

#include <memory>
#include <stdexcept>

namespace opossum {

TransactionContext::TransactionContext(const TransactionID transaction_id, const CommitID last_commit_id)
    : _transaction_id{transaction_id},
      _last_commit_id{last_commit_id},
      _phase{TransactionPhase::Active},
      _num_active_operators{0} {}

TransactionID TransactionContext::transaction_id() const { return _transaction_id; }
CommitID TransactionContext::last_commit_id() const { return _last_commit_id; }

CommitID TransactionContext::commit_id() const {
  if (_commit_context == nullptr) {
    throw std::logic_error("TransactionContext cid only available after commit context has been created.");
  }

  return _commit_context->commit_id();
}

TransactionPhase TransactionContext::phase() const { return _phase; }

std::shared_ptr<CommitContext> TransactionContext::commit_context() { return _commit_context; }

void TransactionContext::on_operator_started() { ++_num_active_operators; }

void TransactionContext::on_operator_finished() {
  const auto num_before = _num_active_operators--;
  if (num_before == 0) {
    throw std::logic_error("Bug detected");
  }

  if (num_before == 1) {
    _active_operators_cv.notify_all();
  }
}

void TransactionContext::wait_for_active_operators_to_finish() const {
  std::unique_lock<std::mutex> lock(_active_operators_mutex);
  _active_operators_cv.wait(lock, [&] { return _num_active_operators != 0; });
}

}  // namespace opossum
