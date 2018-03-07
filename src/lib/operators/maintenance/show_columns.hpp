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
  std::shared_ptr<const Table> _on_execute() override;

  std::shared_ptr<AbstractOperator> _on_recreate(
      const std::vector<AllParameterVariant>& args, const std::shared_ptr<AbstractOperator>& recreated_input_left,
      const std::shared_ptr<AbstractOperator>& recreated_input_right) const override;

 private:
  std::string _table_name;
};
}  // namespace opossum
