#include "transaction_manager.hpp"

#include "commit_context.hpp"
#include "storage/mvcc_data.hpp"
#include "transaction_context.hpp"
#include "utils/assert.hpp"

namespace opossum {

TransactionManager::TransactionManager()
    : _next_transaction_id{INITIAL_TRANSACTION_ID},
      _last_commit_id{INITIAL_COMMIT_ID},
      _last_commit_context{std::make_shared<CommitContext>(INITIAL_COMMIT_ID)} {}

TransactionManager::~TransactionManager() {
  Assert(_active_snapshot_commit_ids.empty(),
         "Some transactions do not seem to have finished yet as they are still registered as active.");
}

TransactionManager& TransactionManager::operator=(TransactionManager&& transaction_manager) noexcept {
  _next_transaction_id = transaction_manager._next_transaction_id.load();
  _last_commit_id = transaction_manager._last_commit_id.load();
  _last_commit_context = transaction_manager._last_commit_context;
  _active_snapshot_commit_ids = transaction_manager._active_snapshot_commit_ids;
  return *this;
}

CommitID TransactionManager::last_commit_id() const { return _last_commit_id; }

std::shared_ptr<TransactionContext> TransactionManager::new_transaction_context(bool is_auto_commit) {
  const TransactionID snapshot_commit_id = _last_commit_id;
  return std::make_shared<TransactionContext>(_next_transaction_id++, snapshot_commit_id, is_auto_commit);
}

void TransactionManager::_register_transaction(const CommitID snapshot_commit_id) {
  std::unique_lock<std::mutex> lock(_mutex_active_snapshot_commit_ids);
  _active_snapshot_commit_ids.insert(snapshot_commit_id);
}

void TransactionManager::_deregister_transaction(const CommitID snapshot_commit_id) {
  std::unique_lock<std::mutex> lock(_mutex_active_snapshot_commit_ids);

  auto it = std::find(_active_snapshot_commit_ids.begin(), _active_snapshot_commit_ids.end(), snapshot_commit_id);

  if (it != _active_snapshot_commit_ids.end()) {
    _active_snapshot_commit_ids.erase(it);
    return;
  }

  Assert(
      it == _active_snapshot_commit_ids.end(),
      "Could not find snapshot_commit_id in TransactionManager's _active_snapshot_commit_ids. Therefore, the removal "
      "failed and the function should not have been called.");
}

std::optional<CommitID> TransactionManager::get_lowest_active_snapshot_commit_id() const {
  std::unique_lock<std::mutex> lock(_mutex_active_snapshot_commit_ids);

  if (_active_snapshot_commit_ids.empty()) {
    return std::nullopt;
  }

  auto it = std::min_element(_active_snapshot_commit_ids.begin(), _active_snapshot_commit_ids.end());
  return *it;
}

/**
 * Logic of the lock-free algorithm
 *
 * Let’s say n threads call this method simultaneously. They all enter the main while-loop.
 * Eventually they reach the point where they try to set the successor of _last_commit_context
 * (pointed to by current_context). Only one of them will succeed and will be able to pass the
 * following if statement. The rest continues with the loop and will now try to get the latest
 * context, which does not have a successor. As long as the thread that succeeded setting
 * the next commit context has not finished updating _last_commit_context, they are stuck in
 * the small while-loop. As soon as it is done, _last_commit_context will point to a commit
 * context with no successor and they will be able to leave this loop.
 */
std::shared_ptr<CommitContext> TransactionManager::_new_commit_context() {
  auto current_context = std::atomic_load(&_last_commit_context);
  auto next_context = std::shared_ptr<CommitContext>();

  auto success = false;
  while (!success) {
    while (current_context->has_next()) {
      current_context = std::atomic_load(&_last_commit_context);
    }

    next_context = std::make_shared<CommitContext>(current_context->commit_id() + 1u);

    success = current_context->try_set_next(next_context);

    if (!success) continue;

    /**
     * Only one thread at a time can ever reach this code since only one thread
     * succeeds to set _last_commit_context’s successor.
     */
    success = std::atomic_compare_exchange_strong(&_last_commit_context, &current_context, next_context);

    Assert(success, "Invariant violated.");
  }

  return next_context;
}

void TransactionManager::_try_increment_last_commit_id(const std::shared_ptr<CommitContext>& context) {
  auto current_context = context;

  while (current_context->is_pending()) {
    auto expected_last_commit_id = current_context->commit_id() - 1;

    if (!_last_commit_id.compare_exchange_strong(expected_last_commit_id, current_context->commit_id())) return;

    current_context->fire_callback();

    if (!current_context->has_next()) return;

    current_context = current_context->next();
  }
}

}  // namespace opossum
