#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <unordered_set>

#include "types.hpp"

/**
 * MVCC overview
 *
 * A good description of MVCC which we used as basis for our implementation is given here:
 * http://15721.courses.cs.cmu.edu/spring2016/papers/schwalb-imdm2014.pdf
 *
 * Conceptually, the idea is that each row has additional "columns" which are used to mark rows as locked for a
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
 * a copy of the current last commit ID, stored as snapshot_commit_id, which represents a snapshot of the database. The
 * snapshot commit ID together with the MVCC data is used to filter out any changes made after the creation
 * transaction context.
 *
 * TransactionContext contains data used by a transaction, mainly its ID, the snapshot commit ID explained above, and,
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
class TransactionManager : public Noncopyable {
  friend class TransactionManagerTest;

 public:
  CommitID last_commit_id() const;

  /**
   * Creates a new transaction context
   */
  std::shared_ptr<TransactionContext> new_transaction_context();

  /**
   * Returns the lowest snapshot-commit-id currently used by a transaction.
   */
  std::optional<CommitID> get_lowest_active_snapshot_commit_id() const;

 private:
  TransactionManager();
  ~TransactionManager();

  friend class Hyrise;
  friend class TransactionContext;

  TransactionManager& operator=(TransactionManager&& transaction_manager) noexcept;

  std::shared_ptr<CommitContext> _new_commit_context();
  void _try_increment_last_commit_id(const std::shared_ptr<CommitContext>& context);

  /**
   * The TransactionManager keeps track of issued snapshot-commit-ids,
   * which are in use by unfinished transactions.
   * The following two functions are used to keep the multiset of active
   * snapshot-commit-ids up to date.
   */
  void _register_transaction(CommitID snapshot_commit_id);
  void _deregister_transaction(CommitID snapshot_commit_id);

  std::atomic<TransactionID> _next_transaction_id;

  std::atomic<CommitID> _last_commit_id;
  // We use commit_id=0 for rows that were inserted and then rolled back. Also, this can be used for rows that have
  // been there "from the beginning of time".
  static constexpr auto INITIAL_COMMIT_ID = CommitID{1};

  std::shared_ptr<CommitContext> _last_commit_context;

  mutable std::mutex _mutex_active_snapshot_commit_ids;
  std::unordered_multiset<CommitID> _active_snapshot_commit_ids;
};
}  // namespace opossum
