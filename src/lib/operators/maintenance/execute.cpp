#include "execute.hpp"

#include "expression/evaluation/expression_evaluator.hpp"
#include "optimizer/optimizer.hpp"
#include "storage/storage_manager.hpp"
#include "logical_query_plan/lqp_translator.hpp"
#include "resolve_type.hpp"

namespace opossum {

Execute::Execute(const std::string& name, const std::vector<std::shared_ptr<AbstractExpression>>& parameters):
  AbstractReadOnlyOperator(OperatorType::Execute), _name(name), _parameters(parameters) {

}

const std::string Execute::name() const {
  return "Execute";
}

const std::string Execute::description(DescriptionMode description_mode) const {
  auto stream = std::ostringstream{};
  stream << "[Execute] '" << _name << "' (";
  for (auto parameter_idx = size_t{0}; parameter_idx < _parameters.size(); ++parameter_idx) {
    stream << _parameters[parameter_idx]->as_column_name();
    if (parameter_idx + 1 < _parameters.size()) {
      stream << ", ";
    }
  }
  stream << ")";

  return stream.str();
}

std::shared_ptr<const Table> Execute::_on_execute() {
//  /**
//   * TODO(moritz)
//   *    How to specify Scheduling and Optimizer from the outside? This operator just using the default Optimizer and
//   *    not scheduling at all is not satisfying
//   */
//
//  const auto prepared_statement = StorageManager::get().get_prepared_statement(_name);
//  Assert(_parameters.size() == prepared_statement->parameter_ids.size(), "Incorrect number of parameters supplied");
//
//  const auto unoptimized_lqp = prepared_statement->lqp;
//  const auto optimized_lqp = Optimizer::create_default_optimizer()->optimize(unoptimized_lqp);
//
//  auto parameters = std::unordered_map<ParameterID, AllTypeVariant>{};
//  for (auto parameter_idx = size_t{0}; parameter_idx < _parameters.size(); ++parameter_idx) {
//    const auto parameter_id = prepared_statement->parameter_ids[parameter_idx];
//
//    resolve_data_type(_parameters[parameter_idx]->data_type(), [&](const auto data_type_t) {
//      using ColumnDataType = typename decltype(data_type_t)::type;
//
//      const auto parameter_result = ExpressionEvaluator{}.evaluate_expression_to_result<ColumnDataType>(*_parameters[parameter_idx]);
//      Assert(parameter_result->size() == 1u, "Parameter expression did not evaluate to a single row");
//
//      const auto parameter_value = parameter_result->is_null(0) ? AllTypeVariant{} : AllTypeVariant{parameter_result->value(0)};
//
//      parameters.emplace(parameter_id, parameter_result);
//    });
//  }
//
//  const auto pqp = LQPTranslator{}.translate_node(optimized_lqp);
//  pqp->set_parameters(parameters);
//  pqp->execute();
//
//  return pqp->get_output();
return nullptr;
}

void Execute::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) { }

std::shared_ptr<AbstractOperator> Execute::_on_deep_copy(
const std::shared_ptr<AbstractOperator>& copied_input_left,
const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<Execute>(_name, _parameters);
}


}  // namespace opossum
