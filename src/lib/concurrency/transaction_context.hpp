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
 * Overview of the different transaction phases
 *
 *  +--------+
 *  | Active |
 *  +--------+
 *      |
 *   Execute operators ---------------+
 *      |                             |
 *      | IF (an operator failed)     | ELSE
 *      |                             |
 *  +--------+                     +------------+
 *  | Failed |                     | Committing |
 *  +--------+                     +------------+
 *      |                             |
 *   Rollback operators             Commit operators
 *      |                             |
 *  +------------+                 +---------+
 *  | RolledBack |                 | Pending |
 *  +------------+                 +---------+
 *                                    |
 *                                  Wait for all previous
 *                                  transaction to be committed
 *                                    |
 *                                 +-----------+
 *                                 | Committed |
 *                                 +-----------+
 */
enum class TransactionPhase {
  Active,      // Transaction has just been created. Operators may be executed.
  Failed,      // One of the operators failed. Transaction needs to be rolled back.
  RolledBack,  // Transaction has been rolled back.
  Committing,  // Commit ID has been assigned. Operators may commit records.
  Pending,     // Transaction has been marked as pending and is ready to be committed.
  Committed    // Transaction has been committed.
};

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
   * @defgroup Lifetime management
   * @{
   */

  /**
   * Sets the transaction phase to Failed.
   * Should be called if an operator fails.
   *
   * @returns false if it is already in phase Failed.
   */
  bool mark_as_failed();

  /**
   * Sets the transaction phase to RolledBack.
   * All registered operators need to have been
   * rolled back in advance.
   *
   * @returns false if it is already in phase RolledBack.
   */
  bool mark_as_rolled_back();

  /**
   * Sets transaction phase to Committing.
   * Creates a new commit context and assigns a new commit id.
   * All operators within this context must be finished and
   * none of the registered operators should have failed when
   * calling this function.
   *
   * @returns false if it is already in phase Committing.
   */
  bool prepare_commit();

  /**
   * Sets transaction phase to Pending.
   * Tries to commit transaction and all following
   * transactions also marked as “pending”. If there are
   * uncommitted transaction with a smaller commit id, it
   * will be committed after those.
   *
   * @param callback called when transaction is committed
   * @returns false if is already in phase Pending.
   */
  bool commit(std::function<void(TransactionID)> callback = nullptr);

  /**@}*/

  /**
   * @defgroup Read/Write operator helpers
   *
   * This methods call commit_records() / rollback_records()
   * on all registered, i.e. previously executed read/write,
   * operators. These methods can be scheduled by wrapping
   * them in a JobTask. If commits and rollbacks should happen
   * individually for each operator, rw_operators() can be
   * called to retrieve a list of all registered operators.
   *
   * Note: These methods were previously wrapped in classes
   *       of type AbstractReadWriteOperators
   *
   * @{
   */

  void commit_operators();
  void rollback_operators();

  /**@}*/

  /**
   * Add an operator to the list of read-write operators.
   * Update must not call this because it consists of a Delete and an Insert, which call this themselves.
   */
  void register_rw_operator(std::shared_ptr<AbstractReadWriteOperator> op) { _rw_operators.push_back(op); }

  std::vector<std::shared_ptr<AbstractReadWriteOperator>> rw_operators() const { return _rw_operators; }

  /**
   * @defgroup Update the counter of active operators
   * @{
   */
  void on_operator_started();
  void on_operator_finished();
  /**@}*/

  void wait_for_active_operators_to_finish() const;

 private:
  /**
   * Throws an exception if the transition fails and
   * has not been already in the target phase.
   */
  bool _transition(TransactionPhase from_phase, TransactionPhase to_phase);

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
