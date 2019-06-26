#include "create_prepared_plan.hpp"

#include "hyrise.hpp"
#include "storage/prepared_plan.hpp"

namespace opossum {

CreatePreparedPlan::CreatePreparedPlan(const std::string& prepared_plan_name,
                                       const std::shared_ptr<PreparedPlan>& prepared_plan)
    : AbstractReadOnlyOperator(OperatorType::CreatePreparedPlan),
      _prepared_plan_name(prepared_plan_name),
      _prepared_plan(prepared_plan) {}

const std::string CreatePreparedPlan::name() const { return "CreatePreparedPlan"; }

const std::string CreatePreparedPlan::description(DescriptionMode description_mode) const {
  std::stringstream stream;
  stream << name() << " '" << _prepared_plan_name << "' {\n";
  stream << *_prepared_plan;
  stream << "}";

  return stream.str();
}

std::shared_ptr<PreparedPlan> CreatePreparedPlan::prepared_plan() const { return _prepared_plan; }

const std::string& CreatePreparedPlan::prepared_plan_name() const { return _prepared_plan_name; }

std::shared_ptr<const Table> CreatePreparedPlan::_on_execute() {
  Hyrise::get().storage_manager.add_prepared_plan(_prepared_plan_name, _prepared_plan);
  return nullptr;
}

void CreatePreparedPlan::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

std::shared_ptr<AbstractOperator> CreatePreparedPlan::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<CreatePreparedPlan>(_prepared_plan_name, _prepared_plan->deep_copy());
}

}  // namespace opossum
