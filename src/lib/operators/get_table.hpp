#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_read_only_operator.hpp"

namespace opossum {

// operator to retrieve a table from the StorageManager by specifying its name
class GetTable : public AbstractReadOnlyOperator {
 public:
  explicit GetTable(const std::string& name);

  const std::string name() const override;
  const std::string description() const override;
  uint8_t num_in_tables() const override;
  uint8_t num_out_tables() const override;

  const std::string& table_name() const;

  std::shared_ptr<AbstractOperator> recreate(const std::vector<AllParameterVariant>& args) const override;

 protected:
  std::shared_ptr<const Table> _on_execute() override;

  // name of the table to retrieve
  const std::string _name;
};
}  // namespace opossum
