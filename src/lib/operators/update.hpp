#pragma once

#include <memory>
#include <string>

#include "abstract_read_write_operator.hpp"

namespace opossum {

class Delete;
class Insert;

/**
 * Operator that updates a subset of columns of a number of rows and from one table with values supplied in another.
 * The first input table must consist of Reference Columns and specifies which rows and columns of the referenced table
 * should be updated.
 * The second input table must have the exact same column layout and number of rows as the first table and contains the
 * data that is used to update the rows specified by the first table.
 * Expects both input tables to only have one chunk.
 *
 * Assumption: The input has been validated before.
 */
class Update : public AbstractReadWriteOperator {
 public:
  explicit Update(const std::string& table_name, std::shared_ptr<AbstractOperator> table_to_update,
                  std::shared_ptr<AbstractOperator> update_values);

  ~Update();

  void commit_records(const CommitID cid) override;
  void rollback_records() override;

  const std::string name() const override;
  uint8_t num_in_tables() const override;

 protected:
  std::shared_ptr<const Table> on_execute(TransactionContext* context) override;
  bool _execution_input_valid(const TransactionContext* context) const;

 protected:
  const std::string _table_name;
  std::unique_ptr<Delete> _delete;
  std::unique_ptr<Insert> _insert;
};
}  // namespace opossum
