#pragma once

#include <atomic>
#include <condition_variable>
#include <memory>
#include <vector>

#include "types.hpp"

namespace opossum {

class AbstractReadWriteOperator;
class CommitContext;

/**
 * @brief Overview of the different transaction phases
 *
 *  +--------+
 *  | Active |
 *  +--------+
 *      |
 *   Execute operators ---------------+
 *      |                             |
 *      | IF (an operator failed)     | ELSE
 *      |                             |
 *  +---------+                    +------------+
 *  | Aborted |                    | Committing |
 *  +---------+                    +------------+
 *      |                             |
 *   Rollback operators             Commit operators
 *      |                             |
 *  +------------+                  Wait for all previous
 *  | RolledBack |                  transaction to be committed
 *  +------------+                    |
 *                                 +-----------+
 *                                 | Committed |
 *                                 +-----------+
 */
enum class TransactionPhase {
  Active,      // Transaction has just been created. Operators may be executed.
  Aborted,     // One of the operators failed. Transaction needs to be rolled back.
  RolledBack,  // Transaction has been rolled back.
  Committing,  // Commit ID has been assigned. Operators may commit records.
  Committed    // Transaction has been committed.
};

/**
 * @brief Representation of a transaction
 */
class TransactionContext : public std::enable_shared_from_this<TransactionContext> {
  friend class TransactionManager;
  // In order to simulate a multi-threaded edge case single-threadedly, the test needs to access private members.
  friend class ConstraintsTest;

 public:
  TransactionContext(const TransactionID transaction_id, const CommitID snapshot_commit_id);
  ~TransactionContext();

  /**
   * The transaction id used among others to lock records in tables.
   */
  TransactionID transaction_id() const;

  /**
   * The snapshot commit id represents the snapshot in time of the database
   * that the transaction is able to see and access.
   */
  CommitID snapshot_commit_id() const;

  /**
   * The commit id that this transaction has once it is committed. This is the one that is written to the
   * begin/end commit ids of rows modified by this transaction.
   * Only available after TransactionManager::prepare_commit has been called
   */
  CommitID commit_id() const;

  /**
   * Returns the current phase of the transaction
   */
  TransactionPhase phase() const;

  /**
   * Returns true if rollback has been called.
   */
  bool aborted() const;

  /**
   * Aborts and rolls back the transaction.
   *
   * @returns false if called a second time
   */
  bool rollback();

  /**
   * Commits the transaction.
   *
   * @param callback called when transaction is actually committed
   * @return false if called a second time
   */
  bool commit_async(const std::function<void(TransactionID)>& callback);

  /**
   * Commits the transaction.
   *
   * Blocks until transaction is actually committed.
   *
   * @return false if called a second time
   */
  bool commit();

  /**
   * Add an operator to the list of read-write operators.
   * Update must not call this because it consists of a Delete and an Insert, which call this themselves.
   */
  void register_read_write_operator(std::shared_ptr<AbstractReadWriteOperator> op) { _rw_operators.push_back(op); }

  /**
   * @defgroup Update the counter of active operators
   * @{
   */
  void on_operator_started();
  void on_operator_finished();
  /**@}*/

 private:
  /**
   * @defgroup Lifetime management
   * @{
   */

  /**
   * Sets the transaction phase to Aborted.
   * Should be called if an operator fails.
   *
   * @returns false if called a second time.
   */
  bool _abort();

  /**
   * Sets the transaction phase to RolledBack.
   */
  void _mark_as_rolled_back();

  /**
   * Sets transaction phase to Committing.
   * Creates a new commit context and assigns a new commit id.
   * All operators within this context must be finished and
   * none of the registered operators should have failed when
   * calling this function.
   *
   * @return false if called a second time.
   */
  bool _prepare_commit();

  /**
   * Sets transaction phase to Pending.
   * Tries to commit transaction and all following
   * transactions also marked as “pending”. If there are
   * uncommitted transaction with a smaller commit id, it
   * will be committed after those.
   *
   * @param callback called when transaction is committed
   */
  void _mark_as_pending_and_try_commit(std::function<void(TransactionID)> callback);

  /**@}*/

  void _wait_for_active_operators_to_finish() const;

  /**
   * Throws an exception if the transition fails and
   * has not been already in phase to_phase or end_phase.
   */
  bool _transition(TransactionPhase from_phase, TransactionPhase to_phase, TransactionPhase end_phase);

 private:
  const TransactionID _transaction_id;
  const CommitID _snapshot_commit_id;
  std::vector<std::shared_ptr<AbstractReadWriteOperator>> _rw_operators;

  std::atomic<TransactionPhase> _phase;
  std::shared_ptr<CommitContext> _commit_context;

  std::atomic_size_t _num_active_operators;

  mutable std::condition_variable _active_operators_cv;
  mutable std::mutex _active_operators_mutex;
};
}  // namespace opossum
