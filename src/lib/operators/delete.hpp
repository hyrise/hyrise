#pragma once

#include <memory>
#include <string>

#include "abstract_modifying_operator.hpp"
#include "table_scan.hpp"

namespace opossum {

// operator to retrieve a table from the StorageManager by specifying its name
class Delete : public AbstractModifyingOperator {
 public:
  explicit Delete(const std::shared_ptr<const TableScan>& table_scan);
  std::shared_ptr<const Table> on_execute(const TransactionContext* context) override;

  const std::string name() const override;
  uint8_t num_in_tables() const override;
  uint8_t num_out_tables() const override;
};
}  // namespace opossum
