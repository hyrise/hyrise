#include <thread>
#include "SQLParser.h"
#include "hyrise.hpp"
#include "lossless_cast.hpp"
#include "postgres_handler.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/current_scheduler.hpp"
#include "sql/sql_pipeline.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "sql/sql_pipeline_statement.hpp"
#include "sql/sql_translator.hpp"
#include "storage/table.hpp"
#include "expression/value_expression.hpp"
#include "types.hpp"

namespace opossum {

// Copy paste
std::vector<RowDescription> build_row_description(std::shared_ptr<const Table> table) {
  // If there is no result table, e.g. after an INSERT command, we cannot send row data
  if (!table) return std::vector<RowDescription>();

  std::vector<RowDescription> result;

  const auto& column_names = table->column_names();
  const auto& column_types = table->column_data_types();

  for (auto column_id = 0u; column_id < table->column_count(); ++column_id) {
    uint32_t object_id;
    int32_t type_id;

    switch (column_types[column_id]) {
      case DataType::Int:
        object_id = 23;
        type_id = 4;
        break;
      case DataType::Long:
        object_id = 20;
        type_id = 8;
        break;
      case DataType::Float:
        object_id = 700;
        type_id = 4;
        break;
      case DataType::Double:
        object_id = 701;
        type_id = 8;
        break;
      case DataType::String:
        object_id = 25;
        type_id = -1;
        break;
      default:
        Fail("Bad DataType");
    }

    result.emplace_back(RowDescription{column_names[column_id], object_id, type_id});
  }
  return result;
}

uint64_t send_query_response(std::shared_ptr<const Table> table, PostgresHandler& postgres_handler) {
  const auto column_count = table->column_count();
  auto attribute_strings = std::vector<std::string>(column_count);
  const auto chunk_count = table->chunk_count();

  // for (const auto& chunk : table->chunks()) {
  for (ChunkID chunk_id{0}; chunk_id < chunk_count; chunk_id++) {
    const auto& chunk = table->get_chunk(chunk_id);
    const auto chunk_size = chunk->size();
    const auto& segments = chunk->segments();
    for (ChunkOffset current_chunk_offset{0}; current_chunk_offset < chunk_size; ++current_chunk_offset) {
      for (size_t segment_counter = 0; segment_counter < segments.size(); segment_counter++) {
        const auto& attribute_value = (*segments[segment_counter])[current_chunk_offset];
        attribute_strings[segment_counter] = lossless_variant_cast<pmr_string>(attribute_value).value();
      }
      postgres_handler.send_data_row(attribute_strings);
    }
  }
  return table->row_count();
}

std::pair<std::shared_ptr<const Table>, std::shared_ptr<const AbstractOperator>> execute_pipeline(
    const std::string& sql) {
  auto sql_pipeline = std::make_shared<SQLPipeline>(SQLPipelineBuilder{sql}.create_pipeline());
  const auto [pipeline_status, result_table] = sql_pipeline->get_result_table();

  Assert(pipeline_status == SQLPipelineStatus::Success, "Server cannot handle failed transactions yet");

  return {result_table, sql_pipeline->get_physical_plans().front()};
}

std::string build_command_complete_message(std::shared_ptr<const AbstractOperator> root_operator_type,
                                           const uint64_t row_count) {
  switch (root_operator_type->type()) {
    case OperatorType::Insert: {
      // 0 is ignored OID and 1 inserted row
      return "INSERT 0 1";
    }
    case OperatorType::Update: {
      // We do not return how many rows are affected, because we don't track this
      // information
      return "UPDATE -1";
    }
    case OperatorType::Delete: {
      // We do not return how many rows are affected, because we don't track this
      // information
      return "DELETE -1";
    }
    default:
      // Assuming normal query
      return "SELECT " + std::to_string(row_count);
  }
}

void setup_prepared_plan(const std::string& statement_name, const std::string& query) {
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

  const auto prepared_plan = std::make_shared<PreparedPlan>(prepared_plans[0], sql_translator.parameter_ids_of_value_placeholders());

  Hyrise::get().storage_manager.add_prepared_plan(statement_name, std::move(prepared_plan));
}

std::shared_ptr<AbstractOperator> bind_plan(const std::shared_ptr<PreparedPlan> prepared_plan,
                                            const std::vector<AllTypeVariant>& parameters) {

  Assert(parameters.size() == prepared_plan->parameter_ids.size(), "Prepared statement parameter count mismatch");

  auto parameter_expressions = std::vector<std::shared_ptr<AbstractExpression>>{parameters.size()};
  for (auto parameter_idx = size_t{0}; parameter_idx < parameters.size(); ++parameter_idx) {
    parameter_expressions[parameter_idx] = std::make_shared<ValueExpression>(parameters[parameter_idx]);
  }

  const auto lqp = prepared_plan->instantiate(parameter_expressions);
  const auto pqp = LQPTranslator{}.translate_node(lqp);

  return pqp;
}

}  // namespace opossum
