#pragma once

#include <atomic>
#include <functional>
#include <memory>

#include "types.hpp"
#include "utils/singleton.hpp"

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
class TransactionManager : public Singleton<TransactionManager> {
 public:
  static void reset();

  CommitID last_commit_id() const;

  /**
   * Creates a new transaction context
   */
  std::shared_ptr<TransactionContext> new_transaction_context();

  // TransactionID = 0 means "not set" in the MVCC data. This is the case if the row has (a) just been reserved, but
  // not yet filled with content, (b) been inserted, committed and not marked for deletion, or (c) inserted but
  // deleted in the same transaction (which has not yet committed)
  static constexpr auto INVALID_TRANSACTION_ID = TransactionID{0};
  static constexpr auto INITIAL_TRANSACTION_ID = TransactionID{1};

 private:
  TransactionManager();

  friend class Singleton;
  friend class TransactionContext;

  std::shared_ptr<CommitContext> _new_commit_context();
  void _try_increment_last_commit_id(const std::shared_ptr<CommitContext>& context);

  std::atomic<TransactionID> _next_transaction_id;

  std::atomic<CommitID> _last_commit_id;
  // We use commit_id=0 for rows that were inserted and then rolled back. Also, this can be used for rows that have
  // been there "from the beginning of time".
  static constexpr auto INITIAL_COMMIT_ID = CommitID{1};

  std::shared_ptr<CommitContext> _last_commit_context;
};
}  // namespace opossum
