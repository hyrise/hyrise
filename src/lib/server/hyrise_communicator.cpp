#include "hyrise_communicator.hpp"

#include "SQLParser.h"
#include "expression/value_expression.hpp"
#include "lossless_cast.hpp"
#include "scheduler/current_scheduler.hpp"
#include "sql/sql_pipeline.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "sql/sql_pipeline_statement.hpp"
#include "sql/sql_translator.hpp"

namespace opossum {

uint64_t HyriseCommunicator::send_query_response(std::shared_ptr<const Table> table,
                                                 std::shared_ptr<PostgresHandler> postgres_handler) {
  auto attribute_strings = std::vector<std::string>(table->column_count());
  const auto chunk_count = table->chunk_count();

  for (ChunkID chunk_id{0}; chunk_id < chunk_count; chunk_id++) {
    const auto chunk_size = table->get_chunk(chunk_id)->size();
    const auto& segments = table->get_chunk(chunk_id)->segments();
    for (ChunkOffset current_chunk_offset{0}; current_chunk_offset < chunk_size; ++current_chunk_offset) {
      for (size_t current_segment = 0; current_segment < segments.size(); current_segment++) {
        const auto& attribute_value = (*segments[current_segment])[current_chunk_offset];
        attribute_strings[current_segment] = lossless_variant_cast<pmr_string>(attribute_value).value();
      }
      postgres_handler->send_data_row(attribute_strings);
    }
  }
  return table->row_count();
}

std::pair<std::shared_ptr<const Table>, OperatorType> HyriseCommunicator::execute_pipeline(const std::string& sql) {
  // A simple query command invalidates unnamed statements
  if (Hyrise::get().storage_manager.has_prepared_plan("")) Hyrise::get().storage_manager.drop_prepared_plan("");

  auto sql_pipeline = std::make_shared<SQLPipeline>(SQLPipelineBuilder{sql}.create_pipeline());
  const auto [pipeline_status, result_table] = sql_pipeline->get_result_table();

  Assert(pipeline_status == SQLPipelineStatus::Success, "Server cannot handle failed transactions yet");

  return std::make_pair(result_table, sql_pipeline->get_physical_plans().front()->type());
}

void HyriseCommunicator::setup_prepared_plan(const std::string& statement_name, const std::string& query) {
  // Named prepared statements must be explicitly closed before they can be redefined by another Parse message
  // https://www.postgresql.org/docs/10/static/protocol-flow.html
  if (Hyrise::get().storage_manager.has_prepared_plan(statement_name)) {
    AssertInput(statement_name.empty(),
                "Named prepared statements must be explicitly closed before they can be redefined.");
    Hyrise::get().storage_manager.drop_prepared_plan(statement_name);
  }

  auto pipeline_statement = SQLPipelineBuilder{query}.create_pipeline_statement();
  auto sql_translator = SQLTranslator{UseMvcc::Yes};
  const auto prepared_plans = sql_translator.translate_parser_result(*pipeline_statement.get_parsed_sql_statement());
  Assert(prepared_plans.size() == 1u, "Only a single statement allowed in prepared statement");

  const auto prepared_plan =
      std::make_shared<PreparedPlan>(prepared_plans[0], sql_translator.parameter_ids_of_value_placeholders());

  Hyrise::get().storage_manager.add_prepared_plan(statement_name, std::move(prepared_plan));
}

std::shared_ptr<AbstractOperator> HyriseCommunicator::bind_prepared_plan(
    const PreparedStatementDetails& statement_details) {
  Assert(Hyrise::get().storage_manager.has_prepared_plan(statement_details.statement_name),
         "The specified statement does not exist.");

  const auto prepared_plan = Hyrise::get().storage_manager.get_prepared_plan(statement_details.statement_name);
  Assert(statement_details.parameters.size() == prepared_plan->parameter_ids.size(),
         "Prepared statement parameter count mismatch");

  // TODO(toni): WTH?
  if (statement_details.statement_name.empty())
    Hyrise::get().storage_manager.drop_prepared_plan(statement_details.statement_name);

  auto parameter_expressions = std::vector<std::shared_ptr<AbstractExpression>>{statement_details.parameters.size()};
  for (auto parameter_idx = size_t{0}; parameter_idx < statement_details.parameters.size(); ++parameter_idx) {
    parameter_expressions[parameter_idx] =
        std::make_shared<ValueExpression>(statement_details.parameters[parameter_idx]);
  }

  const auto lqp = prepared_plan->instantiate(parameter_expressions);
  const auto pqp = LQPTranslator{}.translate_node(lqp);

  return pqp;
}

std::shared_ptr<TransactionContext> HyriseCommunicator::get_new_transaction_context() {
  return Hyrise::get().transaction_manager.new_transaction_context();
}

std::shared_ptr<const Table> HyriseCommunicator::execute_prepared_statement(
    std::shared_ptr<AbstractOperator> physical_plan) {
  const auto tasks = OperatorTask::make_tasks_from_operator(physical_plan, CleanupTemporaries::Yes);
  CurrentScheduler::schedule_and_wait_for_tasks(tasks);
  return tasks.back()->get_operator()->get_output();
}

}  // namespace opossum
