#pragma once

#include <cstdint>
#include <memory>
#include <vector>

#include "commit_context.hpp"

#include "types.hpp"

namespace opossum {

class AbstractReadWriteOperator;

enum class TransactionPhase { Active, Aborted, Committing, Committed };

/**
 * @brief Representation of a transaction
 */
class TransactionContext {
  friend class TransactionManager;

 public:
  TransactionContext(const TransactionID transaction_id, const CommitID last_commit_id);
  ~TransactionContext() = default;

  /**
   * The transaction id used among others to lock records in tables.
   */
  TransactionID transaction_id() const;

  /**
   * The last commit id represents the snapshot in time of the database
   * that the transaction is able to see and access.
   */
  CommitID last_commit_id() const;

  /**
   * Only available after TransactionManager::prepare_commit has been called
   */
  CommitID commit_id() const;

  /**
   * Returns the current phase of the transaction
   */
  TransactionPhase phase() const;

  /**
   * Currently only used for tests
   */
  std::shared_ptr<CommitContext> commit_context();

  void register_rw_operator(AbstractReadWriteOperator* op) { _rw_operators.emplace_back(op); }

  std::vector<AbstractReadWriteOperator*> get_rw_operators() const { return _rw_operators; }

 private:
  const TransactionID _transaction_id;
  const CommitID _last_commit_id;
  std::vector<AbstractReadWriteOperator*> _rw_operators;

  TransactionPhase _phase;
  std::shared_ptr<CommitContext> _commit_context;
};
}  // namespace opossum
