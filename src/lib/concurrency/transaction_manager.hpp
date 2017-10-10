#pragma once

#include <atomic>
#include <functional>
#include <memory>

#include "types.hpp"

/**
 * MVCC overview
 *
 * A good description of MVCC which we used as basis for our implementation is given here:
 * http://15721.courses.cs.cmu.edu/spring2016/papers/schwalb-imdm2014.pdf
 *
 * Conceptually, the idea is that each row has additional columns which are used to mark rows as locked for a
 * transaction and to describe when the row was created and deleted to ensure correct visibility. These vectors are
 * written to by AbstractReadWriteOperators, i.e., Insert, Update and Delete.
 *
 * Rows invisible for the current transaction are filtered by the Validate operator.
 *
 * ReadWriteOperators can fail if they detect conflicting writes by other operators. In that case, the transaction must
 * be rolled back. All executed read/write operators’ rollback_records() method needs to called and the transaction be
 * marked as rolled back.
 *
 * The TransactionManager is a thread-safe singleton that hands out TransactionContexts with monotonically increasing
 * IDs and ensures all transactions are committed in the correct order. It also holds a global last commit ID, which is
 * the commit ID of the last transaction that has been committed. When a new transaction context is created, it retains
 * a copy of the current last commit ID, which represents a snaphot of the database. The last commit ID together with
 * the MVCC columns is used to filter out any changes made after the creation transaction context.
 *
 * TransactionContext contains data used by a transaction, mainly its ID, the last commit ID explained above, and,
 * when it enters the commit phase, the TransactionManager gives it a CommitContext, which contains
 * a new commit ID that is used to make its changes visible to others.
 */

namespace opossum {

class CommitContext;
class TransactionContext;

/**
 * The TransactionManager is responsible for a consistent assignment of
 * transaction and commit ids. It also keeps track of the last commit id
 * which represents the current global visibility of records.
 * The TransactionManager is thread-safe.
 */
class TransactionManager : private Noncopyable {
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
   * Helper: Executes a function object within a context and commits or rolls it back afterwards.
   *
   * It calls TransactionContext::rollback_operators() and TransactionContext::commit_operators()
   * and therefore shouldn’t be used when a scheduler is active. Instead, each operator’s rollback
   * or committing should be schedule separately, for example by using JobTask.
   *
   * Usage: Call TransactionContext::rollback() within the
   *        function object if transaction should be rolled back.
   */
  void run_transaction(const std::function<void(std::shared_ptr<TransactionContext>)> &fn);

 private:
  friend class TransactionContext;

  TransactionManager();

  TransactionManager(TransactionManager &&) = delete;
  TransactionManager &operator=(TransactionManager &&) = delete;

  std::shared_ptr<CommitContext> _new_commit_context();
  void _increment_last_commit_id(std::shared_ptr<CommitContext> context);

 private:
  std::atomic<TransactionID> _next_transaction_id;
  std::atomic<CommitID> _last_commit_id;

  std::shared_ptr<CommitContext> _last_commit_context;
};
}  // namespace opossum
