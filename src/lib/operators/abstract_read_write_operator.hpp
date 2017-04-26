#pragma once

#include <cstdint>
#include <memory>
#include <vector>

#include "concurrency/transaction_context.hpp"
#include "operators/abstract_operator.hpp"
#include "storage/table.hpp"

namespace opossum {

/**
 * AbstractReadWriteOperator is the superclass for all operators that need write access to tables.
 * It mainly provides the commit_records and rollback_records methods, which are used by the CommitRecords
 * and RollbackRecords operators, respectively.
 */
class AbstractReadWriteOperator : public AbstractOperator {
 public:
  explicit AbstractReadWriteOperator(const std::shared_ptr<const AbstractOperator> left = nullptr,
                                     const std::shared_ptr<const AbstractOperator> right = nullptr)
      : AbstractOperator(left, right), _execute_failed{false} {}

  void execute() override {
    _transaction_context->on_operator_started();
    _transaction_context->register_rw_operator(this);
    _output = on_execute(_transaction_context);
    _transaction_context->on_operator_finished();
  }

  /**
   * Commits the operator by applying the cid to the mvcc columns for all modified rows and unlocking them. The
   * modifications will be visible as soon as the TransactionManager has completed the commit for this cid.
   * Unlike on_execute, where failures are expected, the commit operation cannot fail.
   */
  virtual void commit_records(const CommitID cid) = 0;

  /**
   * Rolls back the operator by unlocking all modified rows. No other action is necessary since commit_records should
   * have never been called and the modifications were not made visible in the first place.
   * Like commit, the rollback operation cannot fail.
   */
  virtual void rollback_records() = 0;

  /**
   * Returns true if a previous call to on_execute produced an error.
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
   * In that case, execute_failed returns true after on_execute has returned.
   *
   * @returns nullptr, since these operators do not create new intermediate results but modify existing tables
   */
  std::shared_ptr<const Table> on_execute(std::shared_ptr<TransactionContext> context) override = 0;

 protected:
  bool _execute_failed;
};

}  // namespace opossum
