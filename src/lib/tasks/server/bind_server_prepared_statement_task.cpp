#include "bind_server_prepared_statement_task.hpp"

#include "expression/value_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/lqp_translator.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "sql/sql_pipeline.hpp"
#include "storage/prepared_plan.hpp"

namespace opossum {

void BindServerPreparedStatementTask::_on_execute() {
  try {
    Assert(_params.size() == _prepared_plan->parameter_ids.size(), "Prepared statement parameter count mismatch");

    auto parameter_expressions = std::vector<std::shared_ptr<AbstractExpression>>{_params.size()};
    for (auto parameter_idx = size_t{0}; parameter_idx < _params.size(); ++parameter_idx) {
      parameter_expressions[parameter_idx] = std::make_shared<ValueExpression>(_params[parameter_idx]);
    }

    const auto lqp = _prepared_plan->instantiate(parameter_expressions);
    const auto pqp = LQPTranslator{}.translate_node(lqp);

    _promise.set_value(pqp);
  } catch (const std::exception&) {
    _promise.set_exception(boost::current_exception());
  }
}

}  // namespace opossum
