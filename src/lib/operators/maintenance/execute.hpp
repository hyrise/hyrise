#pragma once

#include "operators/abstract_read_only_operator.hpp"
#include "storage/lqp_prepared_statement.hpp"

namespace opossum {

class Execute : public AbstractReadOnlyOperator {
 public:
  Execute(const std::string& name, const std::vector<std::shared_ptr<AbstractExpression>>& parameters);

  const std::string name() const override;
  const std::string description(DescriptionMode description_mode) const override;

 protected:
  std::shared_ptr<const Table> _on_execute() override;

  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

  std::shared_ptr<AbstractOperator> _on_deep_copy(
  const std::shared_ptr<AbstractOperator>& copied_input_left,
  const std::shared_ptr<AbstractOperator>& copied_input_right) const override;

 private:
  const std::string _name;
  const std::vector<std::shared_ptr<AbstractExpression>> _parameters;
};

}
