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
  explicit TableWrapper(const TableCSPtr table);

  const std::string name() const override;

 protected:
  TableCSPtr _on_execute() override;
  AbstractOperatorSPtr _on_recreate(
      const std::vector<AllParameterVariant>& args, const AbstractOperatorSPtr& recreated_input_left,
      const AbstractOperatorSPtr& recreated_input_right) const override;

  // Table to retrieve
  const TableCSPtr _table;
};
}  // namespace opossum
