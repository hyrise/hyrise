#pragma once

#include <memory>
#include <string>

#include "abstract_read_only_operator.hpp"

namespace opossum {

/**
 * Operator that wraps a table.
 */
class TableWrapper : public AbstractReadOnlyOperator {
 public:
  explicit TableWrapper(const std::shared_ptr<Table> table);

  const std::string name() const override;
  uint8_t num_in_tables() const override;
  uint8_t num_out_tables() const override;
  std::shared_ptr<AbstractOperator> recreate() const override {
    throw std::runtime_error("Operator " + this->name() + " does not implement recreation.");
  }

 protected:
  std::shared_ptr<const Table> on_execute() override;

  // Table to retrieve
  const std::shared_ptr<Table> _table;
};
}  // namespace opossum
