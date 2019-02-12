#pragma once

#include "abstract_read_only_operator.hpp"

namespace opossum {

class TableSample : public AbstractReadOnlyOperator {
 public:
  explicit TableSample(const std::shared_ptr<const AbstractOperator>& in, const size_t num_rows);
  
  const std::string name() const override;
  const std::string description(DescriptionMode description_mode) const override;
  
 protected:
  std::shared_ptr<const Table> _on_execute() override;

  std::shared_ptr<AbstractOperator> _on_deep_copy(
  const std::shared_ptr<AbstractOperator>& copied_input_left,
  const std::shared_ptr<AbstractOperator>& copied_input_right) const override;

  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

  const size_t _num_rows;
};

}  // namespace opossum
