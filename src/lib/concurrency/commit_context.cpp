#include <memory>

#include "commit_context.hpp"
#include "utils/assert.hpp"

namespace opossum {

CommitContext::CommitContext(const CommitID commit_id) : _commit_id{commit_id}, _pending{false} {}

CommitID CommitContext::commit_id() const { return _commit_id; }

bool CommitContext::is_pending() const { return _pending; }

void CommitContext::make_pending(const TransactionID transaction_id,
                                 const std::function<void(TransactionID)>& callback) {
  if (callback) {
    _callback = [callback, transaction_id]() { callback(transaction_id); };
  }

  // This line MUST be AFTER setting the callback. Otherwise we run into a race condition while committing, because this
  // is 'pending' but the callback is not there yet.
  _pending = true;
}

void CommitContext::fire_callback() {
  if (_callback) _callback();
}

bool CommitContext::has_next() const { return next() != nullptr; }

std::shared_ptr<CommitContext> CommitContext::next() { return std::atomic_load(&_next); }

std::shared_ptr<const CommitContext> CommitContext::next() const { return std::atomic_load(&_next); }

bool CommitContext::try_set_next(const std::shared_ptr<CommitContext>& next) {
  DebugAssert((next->commit_id() == commit_id() + 1u), "Next commit context's commit id needs to be incremented by 1.");

  if (has_next()) return false;

  auto context_nullptr = std::shared_ptr<CommitContext>();
  return std::atomic_compare_exchange_strong(&_next, &context_nullptr, next);
}

}  // namespace opossum
