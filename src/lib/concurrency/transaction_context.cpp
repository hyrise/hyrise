#include "transaction_context.hpp"

#include <stdexcept>

#include "transaction_manager.hpp"

namespace opossum {

TransactionContext::TransactionContext(const uint32_t tid, const uint32_t lcid)
    : _tid{tid}, _lcid{lcid}, _phase{TransactionPhase::Active} {}

uint32_t TransactionContext::tid() const { return _tid; }
uint32_t TransactionContext::lcid() const { return _lcid; }

uint32_t TransactionContext::cid() const {
  if (!_commit_context) {
    std::logic_error("TransactionContext cid only available after commit context has been created.");
  }

  return _commit_context->cid();
}

TransactionPhase TransactionContext::phase() const { return _phase; }

void TransactionContext::abort() {
  if (_phase != TransactionPhase::Active) {
    std::logic_error("TransactionContext can only be aborted when active.");
  }

  _phase = TransactionPhase::Aborted;
}

void TransactionContext::prepareCommit() {
  if (_phase != TransactionPhase::Active) {
    std::logic_error("TransactionContext can only be prepared for committing when active.");
  }

  auto& manager = TransactionManager::get();

  _commit_context = manager.new_commit_context();
  _phase = TransactionPhase::Committing;
}

void TransactionContext::commit() {
  if (_phase != TransactionPhase::Committing) {
    std::logic_error("TransactionContext can only be committed when active.");
  }

  auto& manager = TransactionManager::get();

  _commit_context->make_pending();
  manager.commit(_commit_context);

  // TODO: update _phase when transaction actually committed?
  _phase = TransactionPhase::Committed;
}

}  // namespace opossum
