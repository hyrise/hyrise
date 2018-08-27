#pragma once

#include <memory>
#include <string>
#include <vector>

#include "logical_query_plan/abstract_lqp_node.hpp"
#include "operators/abstract_read_only_operator.hpp"

namespace opossum {

// maintenance operator for the "CREATE VIEW" sql statement
class DropView : public AbstractReadOnlyOperator {
 public:
  explicit DropView(const std::string& view_name);

  const std::string name() const override;

 protected:
  std::shared_ptr<const Table> _on_execute() override;

  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_input_left,
      const std::shared_ptr<AbstractOperator>& copied_input_right) const override;
  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

 private:
  const std::string _view_name;
};
}  // namespace opossum
