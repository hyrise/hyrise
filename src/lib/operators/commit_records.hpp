#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_read_write_operator.hpp"

#include "utils/assert.hpp"

namespace opossum {

/**
 * Operator to set all MVCC values correctly to prepare a commit
 * of all operators in the current commit context.
 */
class CommitRecords : public AbstractReadWriteOperator {
 public:
  CommitRecords();
  const std::string name() const override;
  uint8_t num_in_tables() const override;
  uint8_t num_out_tables() const override;

  std::shared_ptr<AbstractOperator> recreate(const std::vector<AllParameterVariant> &args) const override {
    Fail("Operator " + this->name() + " does not implement recreation.");
    return {};
  }

  void commit_records(const CommitID cid) override;
  void rollback_records() override;

 protected:
  /**
   * Calls commit_records on all read-write operators. Needs to have prepare_commit called first.
   */
  std::shared_ptr<const Table> _on_execute(std::shared_ptr<TransactionContext> context) override;
};
}  // namespace opossum
