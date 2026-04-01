#pragma once

#include <memory>
#include <string>
#include <unordered_map>

#include "abstract_read_only_operator.hpp"
#include "all_type_variant.hpp"
#include "expression/abstract_expression.hpp"
#include "types.hpp"

namespace hyrise {

class Probe : public AbstractReadOnlyOperator {
 public:
  Probe(const std::shared_ptr<const AbstractOperator>& input_operator, const ColumnID column_id);

  const std::string& name() const override;

  ColumnID column_id() const;

 protected:
  std::shared_ptr<const Table> _on_execute() override;
  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_left_input,
      const std::shared_ptr<AbstractOperator>& /*copied_right_input*/,
      std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const override;
  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

  ColumnID _column_id;
};

}  // namespace hyrise
