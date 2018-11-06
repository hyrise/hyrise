#pragma once

#include "operators/abstract_read_only_operator.hpp"
#include "storage/prepared_plan.hpp"

namespace opossum {

class Prepare : public AbstractReadOnlyOperator {
 public:
  Prepare(const std::string& name, const std::shared_ptr<PreparedPlan>& prepared_plan);

  const std::string name() const override;
  const std::string description(DescriptionMode description_mode) const override;

  std::shared_ptr<PreparedPlan> prepared_plan() const;

 protected:
  std::shared_ptr<const Table> _on_execute() override;

  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_input_left,
      const std::shared_ptr<AbstractOperator>& copied_input_right) const override;

 private:
  const std::string _name;
  const std::shared_ptr<PreparedPlan> _prepared_plan;
};

}  // namespace opossum
