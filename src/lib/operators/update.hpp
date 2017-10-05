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
  explicit Update(const std::string& table_to_update_name, std::shared_ptr<AbstractOperator> fields_to_update,
                  std::shared_ptr<AbstractOperator> update_values);

  ~Update();

  const std::string name() const override;
  uint8_t num_in_tables() const override;
  std::shared_ptr<AbstractOperator> recreate(const std::vector<AllParameterVariant>& args) const override {
    Fail("Operator " + this->name() + " does not implement recreation.");
    return {};
  }

 protected:
  std::shared_ptr<const Table> _on_execute(std::shared_ptr<TransactionContext> context) override;
  bool _execution_input_valid(const std::shared_ptr<TransactionContext>& context) const;

 protected:
  const std::string _table_to_update_name;
  std::shared_ptr<Delete> _delete;
  std::shared_ptr<Insert> _insert;
};
}  // namespace opossum
