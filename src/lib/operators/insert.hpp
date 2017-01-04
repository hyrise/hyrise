#pragma once

#include <memory>
#include <string>

#include "abstract_operator.hpp"
#include "get_table.hpp"

namespace opossum {

// operator to retrieve a table from the StorageManager by specifying its name
class Insert : public AbstractOperator {
 public:
  explicit Insert(std::shared_ptr<GetTable> get_table, std::vector<AllTypeVariant>&& values);
  void execute() override;
  std::shared_ptr<const Table> get_output() const override;

  const std::string name() const override;
  uint8_t num_in_tables() const override;
  uint8_t num_out_tables() const override;

 protected:
  std::shared_ptr<Table> _table;
  std::vector<AllTypeVariant> _values;
};
}  // namespace opossum
