#include "transaction_context.hpp"

#include "transaction_manager.hpp"

namespace opossum {

TransactionContext::TransactionContext(const uint32_t tid, const uint32_t lcid)
    : _tid{tid}, _lcid{lcid}, _phase{TransactionPhase::Active} {}

uint32_t TransactionContext::tid() const { return _tid; }
uint32_t TransactionContext::lcid() const { return _lcid; }

TransactionPhase TransactionContext::phase() const { return _phase; }

void TransactionContext::add_operator(const std::shared_ptr<AbstractModifyingOperator>& op) {
  if (_phase != TransactionPhase::Active) {
    std::logic_error("Operators can only be added when transaction is active.");
  }

  _operators.push_back(op);
}

void TransactionContext::abort() {
  if (_phase != TransactionPhase::Active) {
    std::logic_error("TransactionContext can only be aborted when active.");
  }

  _phase = TransactionPhase::Aborting;

  for (auto& op : _operators) op->abort();

  _phase = TransactionPhase::Aborted;
}

void TransactionContext::commit() {
  if (_phase != TransactionPhase::Active) {
    std::logic_error("TransactionContext can only be committed when active.");
  }

  auto& manager = TransactionManager::get();

  _commit_context = manager.new_commit_context();
  _phase = TransactionPhase::Committing;

  for (auto& op : _operators) op->commit(_commit_context->cid());

  _commit_context->make_pending();
  manager.commit(_commit_context);

  // TODO: update _phase when transaction actually committed?
  _phase = TransactionPhase::Done;
}

}  // namespace opossum
