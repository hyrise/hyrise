#include "transaction.hpp"

#include "transaction_manager.hpp"

namespace opossum {

Transaction::Transaction(const uint32_t tid, const uint32_t lcid)
    : _tid{tid}, _lcid{lcid}, _phase{TransactionPhase::Active} {}

uint32_t Transaction::tid() const { return _tid; }
uint32_t Transaction::lcid() const { return _lcid; }

TransactionPhase Transaction::phase() const { return _phase; }

void Transaction::add_operator(const std::shared_ptr<AbstractModifyingOperator>& op) {
  if (_phase != TransactionPhase::Active) {
    std::logic_error("Operators can only be added when transaction is active.");
  }

  _operators.push_back(op);
}

void Transaction::abort() {
  if (_phase != TransactionPhase::Active) {
    std::logic_error("Transaction can only be aborted when active.");
  }

  _phase = TransactionPhase::Aborting;

  for (auto& op : _operators) op->abort();

  _phase = TransactionPhase::Aborted;
}

void Transaction::commit() {
  if (_phase != TransactionPhase::Active) {
    std::logic_error("Transaction can only be committed when active.");
  }

  auto& manager = TransactionManager::get();

  _commit_context = manager.new_commit_context();
  _phase = TransactionPhase::Committing;

  for (auto& op : _operators) op->commit(_commit_context->cid());

  manager.commit(_commit_context);

  // TODO: update _phase when transaction actually committed?
  _phase = TransactionPhase::Done;
}

}  // namespace opossum
