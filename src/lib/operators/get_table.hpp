#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_read_only_operator.hpp"
#include "concurrency/transaction_context.hpp"
#include "types.hpp"

namespace opossum {

// operator to retrieve a table from the StorageManager by specifying its name
class GetTable : public AbstractReadOnlyOperator {
 public:
  // Convenience constructor without pruning info
  explicit GetTable(const std::string& name);

  // Constructor with pruning info
  GetTable(const std::string& name, const std::vector<ChunkID>& pruned_chunk_ids,
           const std::vector<ColumnID>& pruned_column_ids);

  const std::string name() const override;
  const std::string description(DescriptionMode description_mode) const override;

  const std::string& table_name() const;
  const std::vector<ChunkID>& pruned_chunk_ids() const;
  const std::vector<ColumnID>& pruned_column_ids() const;

  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_input_left,
      const std::shared_ptr<AbstractOperator>& copied_input_right) const override;
  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

 protected:
  std::shared_ptr<const Table> _on_execute() override;

  // name of the table to retrieve
  const std::string _name;
  const std::vector<ChunkID> _pruned_chunk_ids;
  const std::vector<ColumnID> _pruned_column_ids;
};
}  // namespace opossum
