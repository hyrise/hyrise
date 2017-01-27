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
 * It mainly provides the commit and abort methods, which are used by the commit and abort operators, respectively.
 * Since tables are generally passed around as const variables and changing that would propagate all over the place,
 * implementations of this interface need to use const_casts or work around this by other means. const_cast should not
 * result in undefined behaviour since creating a Table that is const at creation time is not useful and should not
 *  happen at all.
 */
class AbstractReadWriteOperator : public AbstractOperator {
 public:
  explicit AbstractReadWriteOperator(const std::shared_ptr<const AbstractOperator> left = nullptr,
                                     const std::shared_ptr<const AbstractOperator> right = nullptr)
      : AbstractOperator(left, right), _execute_failed{false} {}

  void execute(TransactionContext* context) override {
    context->register_rw_operator(this);
    _output = on_execute(context);
  }

  /**
   * Executes the operator. The context parameter is used to lock the rows that should be modified.
   * Any modifications are not visible to other operators (that is, if the Validate operator has been applied properly)
   * until commit has been called on this operator and the transaction manager has finished committing the respective
   * transaction.
   * The execution may fail if the operator attempts to lock rows that have been locked by other operators.
   * In that case, execute_failed returns true after on_execute has returned.
   */
  std::shared_ptr<const Table> on_execute(TransactionContext* context) override = 0;

  /**
   * Commits the operator by applying the cid to the mvcc columns for all modified rows and unlocking them. The
   * modifications will be visible as soon as the TransactionManager has completed the commit for this cid.
   * Unlike on_execute, where failures are expected, the commit operation cannot fail.
   */
  virtual void commit(const uint32_t cid) = 0;

  /**
   * Aborts the operator by unlocking all modified rows. No other action is necessary since commit should have never
   * been called and the modifications were not made visible in the first place.
   * Like commit, the abort operation cannot fail.
   */
  virtual void abort() = 0;

  /**
   * Returns true iff a previous call to on_execute produced an error.
   */
  bool execute_failed() const { return _execute_failed; }

  uint8_t num_out_tables() const override { return 0; };

 protected:
  bool _execute_failed;
};

}  // namespace opossum
