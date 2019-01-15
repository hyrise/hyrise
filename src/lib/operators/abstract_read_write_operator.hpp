#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "abstract_operator.hpp"

#include "concurrency/transaction_context.hpp"
#include "storage/table.hpp"

#include "utils/assert.hpp"

namespace opossum {

enum class ReadWriteOperatorState {
  Pending,     // The operator has been instantiated.
  Executed,    // Execution succeeded.
  Failed,      // Execution failed.
  RolledBack,  // Changes have been rolled back.
  Committed    // Changes have been committed.
};

/**
 * AbstractReadWriteOperator is the superclass of all operators that need write access to tables.
 * It mainly provides the commit_records and rollback_records methods,
 * which are used to commit and rollback changes respectively.
 */
class AbstractReadWriteOperator : public AbstractOperator {
 public:
  explicit AbstractReadWriteOperator(const OperatorType type,
                                     const std::shared_ptr<const AbstractOperator>& left = nullptr,
                                     const std::shared_ptr<const AbstractOperator>& right = nullptr,
                                     const std::string& target_table_name = nullptr);

  void execute() override;

  /**
   * Commits the operator and triggers any potential work following commits.
   */
  void commit_records(const CommitID commit_id);

  /**
   * Rolls back the operator by unlocking all modified rows. No other action is necessary since commit_records should
   * have never been called and the modifications were not made visible in the first place.
   * Like commit, the rollback operation cannot fail.
   */
  void rollback_records();

  /**
   * Returns true if a previous call to _on_execute produced an error.
   */
  bool execute_failed() const;

  const std::string table_name();

  ReadWriteOperatorState state() const;

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

  /**
   * Commits the operator by applying the cid to the mvcc data for all modified rows and unlocking them. The
   * modifications will be visible as soon as the TransactionManager has completed the commit for this cid.
   * Unlike _on_execute, where failures are expected, the commit operation cannot fail.
   */
  virtual void _on_commit_records(const CommitID commit_id) = 0;

  /**
   * Called immediately after commit_records().
   * This is the place to do any work after modifying operators were successful, e.g. updating statistics.
   */
  virtual void _finish_commit() {}

  /**
   * Called by rollback_records.
   */
  virtual void _on_rollback_records() = 0;

  /**
   * This method is used in sub classes in their _on_execute() method.
   *
   * If the execution fails, because for example some records have already been locked,
   * mark_as_failed() is called to signal to AbstractReadWriteOperator that the execution failed.
   */
  void _mark_as_failed();

  const std::string _target_table_name;

 private:
  ReadWriteOperatorState _state;
};

}  // namespace opossum
