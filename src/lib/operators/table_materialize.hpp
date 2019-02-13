#pragma once

#include "abstract_read_only_operator.hpp"
#include "storage/pos_list.hpp"

namespace opossum {

class TableMaterialize : public AbstractReadOnlyOperator {
 public:
  explicit TableMaterialize(const std::shared_ptr<const AbstractOperator>& in);
  
  const std::string name() const override;
  const std::string description(DescriptionMode description_mode) const override;

 protected:
  std::shared_ptr<const Table> _on_execute() override;

  std::shared_ptr<AbstractOperator> _on_deep_copy(
  const std::shared_ptr<AbstractOperator>& copied_input_left,
  const std::shared_ptr<AbstractOperator>& copied_input_right) const override;

  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;
};

}  // namespace opossum
