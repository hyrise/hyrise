#pragma once

#include <atomic>
#include <functional>
#include <memory>

#include "commit_context.hpp"
#include "transaction_context.hpp"

#include "types.hpp"

/**
 * MVCC overview
 *
 * A good description of MVCC which we used as basis for our implementation is given here:
 * http://15721.courses.cs.cmu.edu/spring2016/papers/schwalb-imdm2014.pdf
 *
 * Conceptually, the idea is that each row has additional columns which are used to mark rows as locked for a
 * transaction, and to describe when the row was created and deleted to ensure correct visibility. These vectors are
 * written to by AbstractReadWriteOperators, of which there are mainly Insert, Update and Delete,
 * as well as CommitRecords and RollbackRecords.
 *
 * Rows invisible for the current transaction are filtered by the Validate operator.

 * The CommitRecords operator must be run at the end of each transaction. To complete the process of making changes
 visible,
 * TransactionManager::commit must be called.
 *
 * ReadWriteOperators can fail if they detect conflicting writes by other operators. In that case, the transaction must
 * be rolled back by running the RollbackRecords operator.
 *
 * The TransactionManager is a thread-safe singleton that hands out TransactionContexts with monotonically increasing
 * IDs and ensures all transactions are committed in the correct order. It also holds a global _last_commit_id, which is
 * the commit id of the last transaction that has been committed. When creating a new TransactionContext, it will get
 * get the current _last_commit_id and thus "see" all inserts and deletes that happened up until that commit ID.
 *
 * TransactionContext contains data used by a transaction, mainly its ID, the last commit ID explained above, and,
 * when it enters the commit phase, the TransactionManager gives it a CommitContext, which contains
 * a new commit ID that the CommitRecords operator uses to make its changes visible to others.
 */

namespace opossum {

/**
 * The TransactionManager is responsible for a consistent assignment of
 * transaction and commit ids. It also keeps track of the last commit id
 * which represents the current global visibility of records.
 * The TransactionManager is thread-safe.
 */
class TransactionManager {
 public:
  static TransactionManager &get();
  static void reset();

  TransactionID next_transaction_id() const;
  CommitID last_commit_id() const;

  /**
   * Creates a new transaction context
   */
  std::shared_ptr<TransactionContext> new_transaction_context();

  /**
   * @defgroup Lifetime management of transactions
   * @{
   */

  /**
   * Sets transaction phase to "rolled back"
   */
  void rollback(TransactionContext &context);

  /**
   * Sets transaction phase to "failed"
   */
  void fail(TransactionContext &context);

  /**
   * Creates new commit context and assigns commit id
   * Sets transaction phase to “committing”
   * After calling this function, transaction cannot be
   * rolled back anymore.
   */
  void prepare_commit(TransactionContext &context);

  /**
   * Tries to commit transaction and all following
   * transactions marked as “pending”. Marks transaction
   * as pending if there are uncommitted transaction with
   * a smaller commit id.
   *
   * @param callback called when transaction is committed
   */
  void commit(TransactionContext &context, std::function<void(TransactionID)> callback = nullptr);

  /** @} */

  /**
   * Helper: Create a transaction context, run a function with it and commit the transaction afterwards
   */
  void run_transaction(const std::function<void(std::shared_ptr<TransactionContext>)> &fn);

 private:
  TransactionManager();

  TransactionManager(TransactionManager const &) = delete;
  TransactionManager(TransactionManager &&) = delete;
  TransactionManager &operator=(const TransactionManager &) = delete;
  TransactionManager &operator=(TransactionManager &&) = delete;

  std::shared_ptr<CommitContext> _new_commit_context();
  void _increment_last_commit_id(std::shared_ptr<CommitContext> context);

 private:
  std::atomic<TransactionID> _next_transaction_id;
  std::atomic<CommitID> _last_commit_id;

  std::shared_ptr<CommitContext> _last_commit_context;
};
}  // namespace opossum
