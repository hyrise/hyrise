#pragma once

#include <memory>
#include <string>
#include <vector>

#include "operators/abstract_read_only_operator.hpp"

namespace opossum {

// maintenance operator to get column information for a table
class ShowColumns : public AbstractReadOnlyOperator {
 public:
  explicit ShowColumns(const std::string& table_name);

  const std::string name() const override;

 protected:
  TableCSPtr _on_execute() override;

  AbstractOperatorSPtr _on_recreate(
      const std::vector<AllParameterVariant>& args, const AbstractOperatorSPtr& recreated_input_left,
      const AbstractOperatorSPtr& recreated_input_right) const override;

 private:
  std::string _table_name;
};
}  // namespace opossum
