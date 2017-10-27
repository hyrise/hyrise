#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_read_only_operator.hpp"
#include "utils/assert.hpp"

namespace opossum {

/**
 * Operator that wraps a table.
 */
class TableWrapper : public AbstractReadOnlyOperator {
 public:
  explicit TableWrapper(const std::shared_ptr<const Table> table);

  const std::string name() const override;
  std::shared_ptr<AbstractOperator> recreate(const std::vector<AllParameterVariant>& args = {}) const override;

 protected:
  std::shared_ptr<const Table> _on_execute() override;

  // Table to retrieve
  const std::shared_ptr<const Table> _table;
};
}  // namespace opossum
