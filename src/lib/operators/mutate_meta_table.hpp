#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_read_write_operator.hpp"

namespace opossum {

class TransactionContext;

/**
 * Operator that modifies meta tables.
 * Expects the table name of the table to modify as a string and both
 * the values to modify and the fully modified values.
 */
class MutateMetaTable : public AbstractReadWriteOperator {
 public:
  explicit MutateMetaTable(const std::string& table_name, const MetaTableMutation& mutation_type,
                           const std::shared_ptr<const AbstractOperator>& values_to_modify,
                           const std::shared_ptr<const AbstractOperator>& modification_values);

  const std::string& name() const override;

 protected:
  std::shared_ptr<const Table> _on_execute(std::shared_ptr<TransactionContext> context) override;
  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_input_left,
      const std::shared_ptr<AbstractOperator>& copied_input_right) const override;
  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;
  void _on_commit_records(const CommitID cid) override;
  void _on_rollback_records() override;

 private:
  const std::string _table_name;
  const MetaTableMutation _mutation_type;
};

}  // namespace opossum
