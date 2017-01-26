#pragma once

#include <atomic>
#include <functional>
#include <memory>

#include "commit_context.hpp"
#include "transaction_context.hpp"

#include "types.hpp"

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
  std::unique_ptr<TransactionContext> new_transaction_context();

  /**
   * @defgroup Lifetime management of transactions
   * @{
   */

  /**
   * Sets transaction phase to “aborted”
   */
  void abort(TransactionContext &context);

  /**
   * Creates new commit context and assigns commit id
   * Sets transaction phase to “committing”
   * After calling this function, transaction cannot be
   * aborted anymore.
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
