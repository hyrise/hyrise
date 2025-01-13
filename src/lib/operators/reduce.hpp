#pragma once

#include <memory>

#include "abstract_read_only_operator.hpp"

namespace hyrise {

class Reduce : public AbstractReadOnlyOperator {
 public:
  explicit Reduce(const std::shared_ptr<const AbstractOperator>& input_relation,
                  const std::shared_ptr<const AbstractOperator>& input_filter = nullptr);

  const std::string& name() const override;

 protected:
  std::shared_ptr<const Table> _on_execute() override;
  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;
  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_left_input,
      const std::shared_ptr<AbstractOperator>& /*copied_right_input*/,
      std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const override;
};

}  // namespace hyrise