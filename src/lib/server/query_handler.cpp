#include "query_handler.hpp"

#include "expression/value_expression.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "sql/sql_translator.hpp"

namespace opossum {

std::pair<ExecutionInformation, std::shared_ptr<TransactionContext>> QueryHandler::execute_pipeline(
    const std::string& query, const SendExecutionInfo send_execution_info,
    const std::shared_ptr<TransactionContext>& transactionContext) {
  // A simple query command invalidates unnamed statements
  // See: https://postgresql.org/docs/12/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY
  if (Hyrise::get().storage_manager.has_prepared_plan("")) Hyrise::get().storage_manager.drop_prepared_plan("");

  auto execution_info = ExecutionInformation();
  auto sql_pipeline = SQLPipelineBuilder{query}.with_transaction_context(transactionContext).create_pipeline();

  const auto [pipeline_status, result_table] = sql_pipeline.get_result_table();

  if (pipeline_status == SQLPipelineStatus::Success) {
    execution_info.result_table = result_table;
    execution_info.root_operator = sql_pipeline.get_physical_plans().back()->type();

    if (send_execution_info == SendExecutionInfo::Yes) {
      std::stringstream stream;
      stream << sql_pipeline.metrics();
      execution_info.pipeline_metrics = stream.str();
    }
  } else if (pipeline_status == SQLPipelineStatus::Failure) {
    const std::string failed_statement = sql_pipeline.failed_pipeline_statement()->get_sql_string();
    execution_info.error_message = {
        {PostgresMessageType::HumanReadableError,
         "Transaction conflict, transaction was rolled back. Failed statement: " + failed_statement},
        {PostgresMessageType::SqlstateCodeError, TRANSACTION_CONFLICT}};
  }
  return std::make_pair(execution_info, sql_pipeline.transaction_context());
}

void QueryHandler::setup_prepared_plan(const std::string& statement_name, const std::string& query) {
  // Named prepared statements must be explicitly closed before they can be redefined by another Parse message.
  // An unnamed prepared statement lasts only until the next Parse statement specifying the unnamed statement as
  // destination is issued
  // https://www.postgresql.org/docs/12/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY
  if (Hyrise::get().storage_manager.has_prepared_plan(statement_name)) {
    AssertInput(statement_name.empty(),
                "Named prepared statements must be explicitly closed before they can be redefined.");
    Hyrise::get().storage_manager.drop_prepared_plan(statement_name);
  }

  auto pipeline_statement = SQLPipelineBuilder{query}.create_pipeline_statement();
  auto sql_translator = SQLTranslator{UseMvcc::Yes};
  auto prepared_plans = sql_translator.translate_parser_result(*pipeline_statement.get_parsed_sql_statement());

  // The PostgreSQL communication protocol does not allow more than one prepared statement within the parse message.
  // See note at: https://www.postgresql.org/docs/12/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY
  AssertInput(prepared_plans.size() == 1u, "Only a single statement allowed in prepared statement");

  const auto prepared_plan =
      std::make_shared<PreparedPlan>(prepared_plans[0], sql_translator.parameter_ids_of_value_placeholders());

  Hyrise::get().storage_manager.add_prepared_plan(statement_name, prepared_plan);
}

std::shared_ptr<AbstractOperator> QueryHandler::bind_prepared_plan(const PreparedStatementDetails& statement_details) {
  AssertInput(Hyrise::get().storage_manager.has_prepared_plan(statement_details.statement_name),
              "The specified statement does not exist.");

  const auto prepared_plan = Hyrise::get().storage_manager.get_prepared_plan(statement_details.statement_name);

  auto parameter_expressions = std::vector<std::shared_ptr<AbstractExpression>>{statement_details.parameters.size()};
  for (auto parameter_idx = size_t{0}; parameter_idx < statement_details.parameters.size(); ++parameter_idx) {
    parameter_expressions[parameter_idx] =
        std::make_shared<ValueExpression>(statement_details.parameters[parameter_idx]);
  }

  const auto lqp = prepared_plan->instantiate(parameter_expressions);
  return LQPTranslator{}.translate_node(lqp);
}

std::shared_ptr<const Table> QueryHandler::execute_prepared_plan(
    const std::shared_ptr<AbstractOperator>& physical_plan) {
  const auto tasks = OperatorTask::make_tasks_from_operator(physical_plan, CleanupTemporaries::Yes);
  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(tasks);
  return tasks.back()->get_operator()->get_output();
}

}  // namespace opossum
