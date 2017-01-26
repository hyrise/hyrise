#include "commit_context.hpp"

#include <memory>

namespace opossum {

CommitContext::CommitContext() : CommitContext{0u} {}

CommitContext::CommitContext(const CommitID commit_id) : _commit_id{commit_id}, _pending{false} {}

CommitContext::~CommitContext() = default;

CommitID CommitContext::commit_id() const { return _commit_id; }

bool CommitContext::is_pending() const { return _pending; }

void CommitContext::make_pending(const TransactionID transaction_id, std::function<void(TransactionID)> callback) {
  auto expected = false;
  const auto success = _pending.compare_exchange_strong(expected, true);

  if (success && callback) {
    _callback = [callback, transaction_id]() { callback(transaction_id); };
  }
}

void CommitContext::fire_callback() {
  if (_callback) _callback();
}

bool CommitContext::has_next() const {
  const auto next_copy = std::atomic_load(&_next);
  return next_copy != nullptr;
}

std::shared_ptr<CommitContext> CommitContext::next() { return std::atomic_load(&_next); }

std::shared_ptr<CommitContext> CommitContext::get_or_create_next() {
  if (has_next()) return next();

  auto new_next = std::make_shared<CommitContext>(_commit_id + 1);

  auto context_nullptr = std::shared_ptr<CommitContext>();
  std::atomic_compare_exchange_strong(&_next, &context_nullptr, new_next);

  return std::atomic_load(&_next);
}

}  // namespace opossum
