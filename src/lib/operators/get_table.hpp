#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_read_only_operator.hpp"
#include "types.hpp"

namespace opossum {

// operator to retrieve a table from the StorageManager by specifying its name
class GetTable : public AbstractReadOnlyOperator {
 public:
  explicit GetTable(const std::string& name);

  const std::string name() const override;
  const std::string description(DescriptionMode description_mode) const override;

  const std::string& table_name() const;
  const std::vector<ChunkID>& excluded_chunk_ids() const;

  void set_excluded_chunk_ids(const std::vector<ChunkID>& excluded_chunk_ids);

  AbstractOperatorSPtr _on_recreate(
      const std::vector<AllParameterVariant>& args, const AbstractOperatorSPtr& recreated_input_left,
      const AbstractOperatorSPtr& recreated_input_right) const override;

 protected:
  TableCSPtr _on_execute() override;

  // name of the table to retrieve
  const std::string _name;
  std::vector<ChunkID> _excluded_chunk_ids;
};
}  // namespace opossum
