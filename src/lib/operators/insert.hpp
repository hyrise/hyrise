#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_read_write_operator.hpp"

#include "utils/assert.hpp"

namespace opossum {

class TransactionContext;

/**
 * Operator that inserts a number of rows from one table into another.
 * Expects the table name of the table to insert into as a string and
 * the values to insert in a separate table using the same column layout.
 *
 * Assumption: The input has been validated before.
 * Note: Insert does not support null values at the moment
 */
class Insert : public AbstractReadWriteOperator {
 public:
  explicit Insert(const std::string& target_table_name, const std::shared_ptr<AbstractOperator>& values_to_insert);

  void commit_records(const CommitID cid) override;
  void rollback_records() override;

  const std::string name() const override;
  uint8_t num_in_tables() const override;
  std::shared_ptr<AbstractOperator> recreate(const std::vector<AllParameterVariant>& args) const override {
    Fail("Operator " + this->name() + " does not implement recreation.");
    return {};
  }

 protected:
  std::shared_ptr<const Table> _on_execute(std::shared_ptr<TransactionContext> context) override;

 protected:
  PosList _inserted_rows;

 private:
  const std::string _target_table_name;
  std::shared_ptr<Table> _target_table;
};

}  // namespace opossum
