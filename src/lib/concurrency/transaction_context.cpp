#include "transaction_context.hpp"

#include <memory>
#include <stdexcept>

namespace opossum {

TransactionContext::TransactionContext(const TransactionID transaction_id, const CommitID last_commit_id)
    : _transaction_id{transaction_id}, _last_commit_id{last_commit_id}, _phase{TransactionPhase::Active} {}

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

}  // namespace opossum
