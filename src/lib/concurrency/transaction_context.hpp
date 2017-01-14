#pragma once

#include <cstdint>
#include <memory>

#include "commit_context.hpp"

namespace opossum {

enum class TransactionPhase { Active, Aborted, Committing, Committed };

class TransactionContext {
 public:
  TransactionContext(const uint32_t tid, const uint32_t lcid);
  ~TransactionContext() = default;

  uint32_t tid() const;
  uint32_t lcid() const;

  // only available after prepareCommit has been called.
  uint32_t cid() const;

  TransactionPhase phase() const;

  // sets phase to Aborted
  void abort();

  // creates commit context sets phase to Committing
  void prepareCommit();

  // tries to commit transaction and sets phase to Committed
  void commit();

 private:
  const uint32_t _tid;
  const uint32_t _lcid;

  TransactionPhase _phase;
  std::shared_ptr<CommitContext> _commit_context;
};
}  // namespace opossum
