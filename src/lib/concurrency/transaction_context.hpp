#pragma once

#include <cstdint>
#include <memory>

#include "commit_context.hpp"

namespace opossum {

enum class TransactionPhase { Active, Aborted, Committing, Committed };

class TransactionContext {
  friend class TransactionManager;

 public:
  TransactionContext(const uint32_t tid, const uint32_t lcid);
  ~TransactionContext() = default;

  uint32_t tid() const;
  uint32_t lcid() const;

  // only available after TransactionManager::prepare_commit has been called
  uint32_t cid() const;

  TransactionPhase phase() const;

  std::shared_ptr<CommitContext> commit_context();

 private:
  const uint32_t _tid;
  const uint32_t _lcid;

  TransactionPhase _phase;
  std::shared_ptr<CommitContext> _commit_context;
};
}  // namespace opossum
