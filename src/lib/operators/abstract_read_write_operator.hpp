#pragma once

#include <cstdint>
#include <memory>
#include <vector>

#include "abstract_operator.hpp"

#include "concurrency/transaction_context.hpp"
#include "storage/table.hpp"

#include "utils/assert.hpp"

namespace opossum {

/**
 * AbstractReadWriteOperator is the superclass for all operators that need write access to tables.
 * It mainly provides the commit_records and rollback_records methods, which are used by the CommitRecords
 * and RollbackRecords operators, respectively.
 */
class AbstractReadWriteOperator : public AbstractOperator,
                                  public std::enable_shared_from_this<AbstractReadWriteOperator> {
 public:
  explicit AbstractReadWriteOperator(const std::shared_ptr<const AbstractOperator> left = nullptr,
                                     const std::shared_ptr<const AbstractOperator> right = nullptr)
      : AbstractOperator(left, right), _execute_failed{false} {}

  void execute() override {
    auto transaction_context = _transaction_context.lock();
    DebugAssert(static_cast<bool>(transaction_context), "Probably a bug.");

    transaction_context->on_operator_started();
    _output = _on_execute(transaction_context);
    transaction_context->on_operator_finished();
  }

  /**
   * Commits the operator and triggers any potential work following commits.
   */
  void commit(const CommitID cid) {
    commit_records(cid);
    finish_commit();
  }

  /**
   * Commits the operator by applying the cid to the mvcc columns for all modified rows and unlocking them. The
   * modifications will be visible as soon as the TransactionManager has completed the commit for this cid.
   * Unlike _on_execute, where failures are expected, the commit operation cannot fail.
   */
  virtual void commit_records(const CommitID cid) {}

  /**
   * Called immediately after commit_records().
   * This is the place to do any work after modifying operators were successful, e.g. updating statistics.
   */
  virtual void finish_commit() {}

  /**
   * Rolls back the operator by unlocking all modified rows. No other action is necessary since commit_records should
   * have never been called and the modifications were not made visible in the first place.
   * Like commit, the rollback operation cannot fail.
   */
  virtual void rollback_records() {}

  /**
   * Returns true if a previous call to _on_execute produced an error.
   */
  bool execute_failed() const { return _execute_failed; }

  uint8_t num_out_tables() const override { return 0; };

 protected:
  /**
   * Executes the operator. The context parameter is used to lock the rows that should be modified.
   * Any modifications are not visible to other operators (that is, if the Validate operator has been applied properly)
   * until commit_records has been called on this operator and the transaction manager has finished committing the
   * respective transaction.
   * The execution may fail if the operator attempts to lock rows that have been locked by other operators.
   * In that case, execute_failed returns true after _on_execute has returned.
   *
   * @returns nullptr, since these operators do not create new intermediate results but modify existing tables
   */
  std::shared_ptr<const Table> _on_execute(std::shared_ptr<TransactionContext> context) override = 0;

 protected:
  bool _execute_failed;
};

}  // namespace opossum
