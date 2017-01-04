#pragma once

#include <memory>
#include <string>

#include "abstract_operator.hpp"
#include "table_scan.hpp"

namespace opossum {

// operator to retrieve a table from the StorageManager by specifying its name
class Delete : public AbstractOperator {
 public:
  explicit Delete(const std::shared_ptr<const TableScan>& table_scan);
  void execute() override;
  std::shared_ptr<const Table> get_output() const override;

  const std::string name() const override;
  uint8_t num_in_tables() const override;
  uint8_t num_out_tables() const override;
};
}  // namespace opossum
