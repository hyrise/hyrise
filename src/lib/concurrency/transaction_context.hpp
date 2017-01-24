#pragma once

#include <cstdint>
#include <memory>

#include "commit_context.hpp"

#include "types.hpp"

namespace opossum {

enum class TransactionPhase { Active, Aborted, Committing, Committed };

class TransactionContext {
  friend class TransactionManager;

 public:
  TransactionContext(const TransactionID transaction_id, const CommitID last_commit_id);
  ~TransactionContext() = default;

  TransactionID transaction_id() const;
  CommitID last_commit_id() const;

  // only available after TransactionManager::prepare_commit has been called
  CommitID commit_id() const;

  TransactionPhase phase() const;

  std::shared_ptr<CommitContext> commit_context();

 private:
  const TransactionID _transaction_id;
  const CommitID _last_commit_id;

  TransactionPhase _phase;
  std::shared_ptr<CommitContext> _commit_context;
};
}  // namespace opossum
