#include "transaction_manager.hpp"

namespace opossum {

TransactionManager &TransactionManager::get() {
  static TransactionManager instance;
  return instance;
}

TransactionManager::TransactionManager() : _ntid{1u}, _lcid{0u} {}

std::unique_ptr<TransactionContext> TransactionManager::new_transaction() {
  return std::make_unique<TransactionContext>(_ntid++, _lcid);
}

std::shared_ptr<CommitContext> TransactionManager::new_commit_context() {
  const auto lcc_copy = std::atomic_load(&_lcc);
  if (lcc_copy == nullptr) {
    auto new_context = std::make_shared<CommitContext>();

    auto context_nullptr = std::shared_ptr<CommitContext>();
    const auto success = std::atomic_compare_exchange_strong(&_lcc, &context_nullptr, new_context);

    if (success) return new_context;
  }

  auto current_context = std::atomic_load(&_lcc);
  auto next_context = std::shared_ptr<CommitContext>();

  auto success = false;
  while (!success) {
    while (current_context->has_next()) {
      current_context = current_context->get_or_create_next();
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

    current_context = current_context->get_or_create_next();
  }
}

}  // namespace opossum
