#include "transaction.hpp"

#include "transaction_manager.hpp"

namespace opossum {

Transaction::Transaction(const uint32_t tid, const uint32_t lcid)
    : _tid{tid}, _lcid{lcid}, _phase{TransactionPhase::Active} {}

uint32_t Transaction::tid() const { return _tid; }
uint32_t Transaction::lcid() const { return _lcid; }

TransactionPhase Transaction::phase() const { return _phase; }

void Transaction::abort() {
  // assert(_phase == TransactionPhase::Active)
  _phase = TransactionPhase::Aborted;
}

void Transaction::prepareCommit() {
  // assert(_phase == TransactionPhase::Active)

  _commit_context = TransactionManager::get().new_commit_context();
  _phase = TransactionPhase::Committing;
}

void Transaction::commit() {
  // assert(_phase == TransactionPhase::Committing)

  // for (const auto& row : _inserted_rows) {
  // write commit id
  // }

  // for (const auto& row : _deleted_rows) {
  // write commit id
  // }

  auto commit_context_copy = _commit_context;
  TransactionManager::get().commit(commit_context_copy);
  _phase = TransactionPhase::Done;
}

}  // namespace opossum
