#include "transaction_context.hpp"

#include <stdexcept>
#include <memory>

namespace opossum {

TransactionContext::TransactionContext(const uint32_t tid, const uint32_t lcid)
    : _tid{tid}, _lcid{lcid}, _phase{TransactionPhase::Active} {}

uint32_t TransactionContext::tid() const { return _tid; }
uint32_t TransactionContext::lcid() const { return _lcid; }

uint32_t TransactionContext::cid() const {
  if (_commit_context == nullptr) {
    throw std::logic_error("TransactionContext cid only available after commit context has been created.");
  }

  return _commit_context->cid();
}

TransactionPhase TransactionContext::phase() const { return _phase; }

std::shared_ptr<CommitContext> TransactionContext::commit_context() { return _commit_context; }

}  // namespace opossum
