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
  explicit TableWrapper(const std::shared_ptr<const Table>& init_table);

  const std::string& name() const override;

  // Table to retrieve
  const std::shared_ptr<const Table> table;

 protected:
  std::shared_ptr<const Table> _on_execute() override;
  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_left_input,
      const std::shared_ptr<AbstractOperator>& copied_right_input) const override;
  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;
};
}  // namespace opossum
