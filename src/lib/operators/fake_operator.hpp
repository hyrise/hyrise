#pragma once

#include <memory>
#include <string>

#include "abstract_read_only_operator.hpp"

namespace opossum {

// operator to retrieve a table from the StorageManager by specifying its name
class FakeOperator : public AbstractReadOnlyOperator {
 public:
  explicit FakeOperator(const std::shared_ptr<Table> table);

  const std::string name() const override;
  uint8_t num_in_tables() const override;
  uint8_t num_out_tables() const override;

 protected:
  std::shared_ptr<const Table> on_execute() override;

  // name of the table to retrieve
  const std::shared_ptr<Table> _table;
};
}  // namespace opossum
