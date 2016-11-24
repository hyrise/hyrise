#pragma once

#include <memory>
#include <string>

#include "abstract_operator.hpp"

namespace opossum {

// operator to retrieve a table from the StorageManager by specifying its name
class GetTable : public AbstractOperator {
 public:
  explicit GetTable(const std::string &name);
  void execute() override;
  std::shared_ptr<const Table> get_output() const override;

  const std::string name() const override;
  uint8_t num_in_tables() const override;
  uint8_t num_out_tables() const override;

 protected:
  // name of the table to retrieve
  const std::string _name;
};
}  // namespace opossum
