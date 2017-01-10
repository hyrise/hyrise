#include "transaction_manager.hpp"

namespace opossum {

TransactionManager &TransactionManager::get() {
  static TransactionManager instance;
  return instance;
}

TransactionManager::TransactionManager() : _ntid{1u}, _lcid{0u}, _lcc{std::make_shared<CommitContext>()} {}

std::unique_ptr<TransactionContext> TransactionManager::new_transaction_context() {
  return std::make_unique<TransactionContext>(_ntid++, _lcid);
}

std::shared_ptr<CommitContext> TransactionManager::new_commit_context() {
  auto current_context = std::atomic_load(&_lcc);
  auto next_context = std::shared_ptr<CommitContext>();

  auto success = false;
  while (!success) {
    while (current_context->has_next()) {
      current_context = current_context->next();
    }

    next_context = current_context->get_or_create_next();
    success = std::atomic_compare_exchange_strong(&_lcc, &current_context, next_context);
  }

  return next_context;
}

void TransactionManager::commit(std::shared_ptr<CommitContext> context) {
  auto current_context = context;

  while (current_context->is_pending()) {
    auto expected_lcid = current_context->cid() - 1;

    if (!_lcid.compare_exchange_strong(expected_lcid, current_context->cid())) return;

    // TODO: send response to client

    if (!current_context->has_next()) return;

    current_context = current_context->next();
  }
}

}  // namespace opossum
