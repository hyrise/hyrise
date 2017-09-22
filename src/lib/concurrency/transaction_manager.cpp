#include "transaction_manager.hpp"

#include <memory>
#include <stdexcept>

#include "commit_context.hpp"
#include "transaction_context.hpp"

#include "operators/commit_records.hpp"
#include "scheduler/operator_task.hpp"
#include "utils/assert.hpp"

namespace opossum {

TransactionManager& TransactionManager::get() {
  static TransactionManager instance;
  return instance;
}

void TransactionManager::reset() {
  auto& manager = get();
  manager._next_transaction_id = 1u;
  manager._last_commit_id = 0u;
  manager._last_commit_context = std::make_shared<CommitContext>(0u);
}

TransactionManager::TransactionManager()
    : _next_transaction_id{1u}, _last_commit_id{0u}, _last_commit_context{std::make_shared<CommitContext>(0u)} {}

TransactionID TransactionManager::next_transaction_id() const { return _next_transaction_id; }

CommitID TransactionManager::last_commit_id() const { return _last_commit_id; }

std::shared_ptr<TransactionContext> TransactionManager::new_transaction_context() {
  return std::make_shared<TransactionContext>(_next_transaction_id++, _last_commit_id);
}

void TransactionManager::rollback(TransactionContext& context) {
  DebugAssert((context._phase == TransactionPhase::Active), "TransactionContext can only be rolled back when active.");

  context._phase = TransactionPhase::RolledBack;
}

void TransactionManager::fail(TransactionContext& context) {
  DebugAssert((context._phase == TransactionPhase::Active), "TransactionContext can only fail when active.");

  context._phase = TransactionPhase::Failed;
}

void TransactionManager::prepare_commit(TransactionContext& context) {
  DebugAssert((context._phase == TransactionPhase::Active),
              "TransactionContext can only be prepared for committing when active.");

  context._commit_context = _new_commit_context();
  context._phase = TransactionPhase::Committing;
}

void TransactionManager::commit(TransactionContext& context, std::function<void(TransactionID)> callback) {
  DebugAssert((context._phase == TransactionPhase::Committing),
              "TransactionContext can only be committed after prepare_commit has been called.");

  auto commit_context = context._commit_context;

  commit_context->make_pending(context.transaction_id(), callback);
  _increment_last_commit_id(commit_context);

  // TODO(EVERYONE): update _phase when transaction actually committed?
  context._phase = TransactionPhase::Committed;
}

void TransactionManager::run_transaction(const std::function<void(std::shared_ptr<TransactionContext>)>& fn) {
  auto transaction_context = new_transaction_context();

  fn(transaction_context);

  // Commit
  this->prepare_commit(*transaction_context);

  auto commit = std::make_shared<CommitRecords>();
  commit->set_transaction_context(transaction_context);

  auto commit_task = std::make_shared<OperatorTask>(commit);
  commit_task->schedule();
  commit_task->join();

  this->commit(*transaction_context);
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

void TransactionManager::_increment_last_commit_id(std::shared_ptr<CommitContext> context) {
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
