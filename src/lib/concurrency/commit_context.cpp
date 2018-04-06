#include <memory>

#include "commit_context.hpp"
#include "utils/assert.hpp"

namespace opossum {

CommitContext::CommitContext(const CommitID commit_id) : _commit_id{commit_id}, _pending{false} {}

CommitID CommitContext::commit_id() const { return _commit_id; }

bool CommitContext::is_pending() const { return _pending; }

void CommitContext::make_pending(const TransactionID transaction_id, std::function<void(TransactionID)> callback) {
  _pending = true;

  if (callback) {
    _callback = [callback, transaction_id]() { callback(transaction_id); };
  }
}

void CommitContext::fire_callback() {
  if (_callback) _callback();
}

bool CommitContext::has_next() const { return next() != nullptr; }

CommitContextSPtr CommitContext::next() { return std::atomic_load(&_next); }

CommitContextCSPtr CommitContext::next() const { return std::atomic_load(&_next); }

bool CommitContext::try_set_next(const CommitContextSPtr& next) {
  DebugAssert((next->commit_id() == commit_id() + 1u), "Next commit context's commit id needs to be incremented by 1.");

  if (has_next()) return false;

  auto context_nullptr = CommitContextSPtr();
  return std::atomic_compare_exchange_strong(&_next, &context_nullptr, next);
}

}  // namespace opossum
