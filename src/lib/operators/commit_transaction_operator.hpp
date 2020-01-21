#pragma once

#include "abstract_operator.hpp"
#include "abstract_read_only_operator.hpp"

namespace opossum {

class CommitTransactionOperator : public AbstractReadOnlyOperator {
 public:
  explicit CommitTransactionOperator();

  const std::string& name() const override;

 protected:
  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_input_left,
      const std::shared_ptr<AbstractOperator>& copied_input_right) const override;
  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;
  std::shared_ptr<const Table> _on_execute() override;
};
}  // namespace opossum
