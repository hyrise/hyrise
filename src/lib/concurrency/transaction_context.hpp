#pragma once

#include <atomic>
#include <condition_variable>
#include <memory>
#include <vector>

#include "commit_context.hpp"

#include "types.hpp"

namespace opossum {

class AbstractReadWriteOperator;

enum class TransactionPhase { Active, Failed, RolledBack, Committing, Committed };

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

  /**
   * Add an operator to the list of read-write operators.
   * Update must not call this because it consists of a Delete and an Insert, which call this themselves.
   */
  void register_rw_operator(std::shared_ptr<AbstractReadWriteOperator> op) { _rw_operators.push_back(op); }

  std::vector<std::shared_ptr<AbstractReadWriteOperator>> get_rw_operators() const { return _rw_operators; }

  /**
   * Update the counter of active operators
   */
  void on_operator_started();
  void on_operator_finished();

  void wait_for_active_operators_to_finish() const;

 private:
  const TransactionID _transaction_id;
  const CommitID _last_commit_id;
  std::vector<std::shared_ptr<AbstractReadWriteOperator>> _rw_operators;

  std::atomic<TransactionPhase> _phase;
  std::shared_ptr<CommitContext> _commit_context;

  std::atomic_size_t _num_active_operators;

  mutable std::condition_variable _active_operators_cv;
  mutable std::mutex _active_operators_mutex;
};
}  // namespace opossum
