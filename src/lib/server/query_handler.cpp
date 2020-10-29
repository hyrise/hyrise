#include "query_handler.hpp"

#include "expression/value_expression.hpp"
#include "optimizer/optimizer.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "sql/sql_translator.hpp"

namespace opossum {

std::pair<ExecutionInformation, std::shared_ptr<TransactionContext>> QueryHandler::execute_pipeline(
    const std::string& query, const SendExecutionInfo send_execution_info,
    const std::shared_ptr<TransactionContext>& transaction_context) {
  // A simple query command invalidates unnamed statements
  // See: https://postgresql.org/docs/12/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY
  if (Hyrise::get().storage_manager.has_prepared_plan("")) Hyrise::get().storage_manager.drop_prepared_plan("");

  DebugAssert(!transaction_context || !transaction_context->is_auto_commit(),
              "Auto-commit transaction contexts should not be passed around this far");

  auto execution_info = ExecutionInformation();
  auto sql_pipeline = SQLPipelineBuilder{query}.with_transaction_context(transaction_context).create_pipeline();

  const auto [pipeline_status, result_table] = sql_pipeline.get_result_table();

  if (pipeline_status == SQLPipelineStatus::Success) {
    execution_info.result_table = result_table;
    execution_info.root_operator_type = sql_pipeline.get_physical_plans().back()->type();

    _handle_transaction_statement_message(execution_info, sql_pipeline);

    if (send_execution_info == SendExecutionInfo::Yes) {
      std::stringstream stream;
      stream << sql_pipeline.metrics();
      execution_info.pipeline_metrics = stream.str();
    }
  } else if (pipeline_status == SQLPipelineStatus::Failure) {
    const std::string failed_statement = sql_pipeline.failed_pipeline_statement()->get_sql_string();
    execution_info.error_message = {{PostgresMessageType::HumanReadableError,
                                     "Transaction conflict, transaction was rolled back. Following statements might "
                                     "have still been sent and executed. Failed statement: " +
                                         failed_statement},
                                    {PostgresMessageType::SqlstateCodeError, TRANSACTION_CONFLICT}};
  }
  return {execution_info, sql_pipeline.transaction_context()};
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

  auto pipeline = SQLPipelineBuilder{query}.create_pipeline();
  const auto& lqps = pipeline.get_unoptimized_logical_plans();

  // The PostgreSQL communication protocol does not allow more than one prepared statement within the parse message.
  // See note at: https://www.postgresql.org/docs/12/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY
  AssertInput(lqps.size() == 1u, "Only a single statement allowed in prepared statement");

  const auto& translation_infos = pipeline.get_sql_translation_infos();

  const auto& lqp = lqps[0];
  const auto& translation_info = translation_infos[0].get();

  auto parameter_ids_of_value_placeholders = translation_info.parameter_ids_of_value_placeholders;
  const auto prepared_plan = std::make_shared<PreparedPlan>(lqp, parameter_ids_of_value_placeholders);

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

  auto lqp = prepared_plan->instantiate(parameter_expressions);
  const auto optimizer = Optimizer::create_default_optimizer();
  lqp = optimizer->optimize(std::move(lqp));

  auto pqp = LQPTranslator{}.translate_node(lqp);

  return pqp;
}

std::shared_ptr<const Table> QueryHandler::execute_prepared_plan(
    const std::shared_ptr<AbstractOperator>& physical_plan) {
  const auto tasks = OperatorTask::make_tasks_from_operator(physical_plan);
  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(tasks);
  return static_cast<const OperatorTask&>(*tasks.back()).get_operator()->get_output();
}

void QueryHandler::_handle_transaction_statement_message(ExecutionInformation& execution_info,
                                                         SQLPipeline& sql_pipeline) {
  // handle custom user feedback (command complete messages) for transaction statements
  auto sql_statement = sql_pipeline.get_parsed_sql_statements().back();
  const auto& statements = sql_statement->getStatements();

  DebugAssert(statements.size() == 1, "SQL statements were not properly split");
  if (statements[0]->isType(hsql::StatementType::kStmtTransaction)) {
    const auto& transaction_statement = dynamic_cast<hsql::TransactionStatement&>(*statements.front());

    switch (transaction_statement.command) {
      case hsql::kBeginTransaction: {
        execution_info.custom_command_complete_message = "BEGIN";
        break;
      }
      case hsql::kCommitTransaction: {
        execution_info.custom_command_complete_message = "COMMIT";
        break;
      }
      case hsql::kRollbackTransaction: {
        execution_info.custom_command_complete_message = "ROLLBACK";
        break;
      }
      default: {
        FailInput("TransactionStatement command not supported");
      }
    }
  }
}
}  // namespace opossum
