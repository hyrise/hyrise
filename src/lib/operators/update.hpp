#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_read_write_operator.hpp"
#include "utils/assert.hpp"

namespace opossum {

class Delete;
class Insert;

/**
 * Operator that updates a subset of columns of a number of rows and from one table with values supplied in another.
 * The first input table must consist of ReferenceColumns and specifies which rows and columns of the referenced table
 * should be updated. This operator uses bag semantics, that is, exactly the referenced cells are updated, and not all
 * rows with similar data.
 * The second input table must have the exact same column layout and number of rows as the first table and contains the
 * data that is used to update the rows specified by the first table.
 *
 * Assumption: The input has been validated before.
 *
 * Note: Update does not support null values at the moment
 */
class Update : public AbstractReadWriteOperator {
 public:
  explicit Update(const std::string& table_to_update_name, AbstractOperatorSPtr fields_to_update,
                  AbstractOperatorSPtr update_values);

  ~Update();

  const std::string name() const override;

 protected:
  TableCSPtr _on_execute(TransactionContextSPtr context) override;
  AbstractOperatorSPtr _on_recreate(
      const std::vector<AllParameterVariant>& args, const AbstractOperatorSPtr& recreated_input_left,
      const AbstractOperatorSPtr& recreated_input_right) const override;
  bool _execution_input_valid(const TransactionContextSPtr& context) const;

  // Commit happens in Insert and Delete operators
  void _on_commit_records(const CommitID cid) override {}

  // Rollback happens in Insert and Delete operators
  void _on_rollback_records() override {}

 protected:
  const std::string _table_to_update_name;
  DeleteSPtr _delete;
  InsertSPtr _insert;
};
}  // namespace opossum
