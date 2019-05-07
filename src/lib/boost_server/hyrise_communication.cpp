#include "SQLParser.h"
#include "postgres_handler.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/current_scheduler.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "sql/sql_pipeline_statement.hpp"
#include "sql/sql_translator.hpp"
#include "storage/table.hpp"
#include "tasks/server/pipeline_execution_task.hpp"
#include "types.hpp"

namespace opossum {

// Copy paste
std::vector<RowDescription> build_row_description(const std::shared_ptr<SQLPipeline> sql_pipeline) {
  auto table = sql_pipeline->get_result_table();

  // If there is no result table, e.g. after an INSERT command, we cannot send
  // row data
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

uint64_t send_query_response(const std::shared_ptr<SQLPipeline> sql_pipeline, PostgresHandler& postgres_handler) {
  auto table = sql_pipeline->get_result_table();
  uint32_t chunk_size;
  auto column_count = table->column_count();
  auto row_strings = std::vector<std::string>(column_count);

  for (const auto& chunk : table->chunks()) {
    chunk_size = chunk->size();
    for (ChunkOffset current_chunk_offset{0}; current_chunk_offset < chunk_size; ++current_chunk_offset) {
      for (ColumnID column_id{0}; column_id < column_count; ++column_id) {
        const auto& segment = chunk->get_segment(column_id);
        row_strings[column_id] = boost::lexical_cast<pmr_string>((*segment)[current_chunk_offset]);
      }
      postgres_handler.send_data_row(row_strings);
    }
  }
  return table->row_count();
}

std::shared_ptr<SQLPipeline> execute_pipeline(const std::string& sql) {
  auto task = std::make_shared<PipelineExecutionTask>(sql);
  task->schedule();

  // blocking and erroneous
  // condition variable as callback here
  auto sql_pipeline = task->get_sql_pipeline();
  if (sql_pipeline->failed_pipeline_statement()) {
    // TODO(toni): error handling
  }
  return sql_pipeline;
}

std::string build_command_complete_message(const std::shared_ptr<SQLPipeline> sql_pipeline, uint64_t row_count) {
  auto root_operator_type = sql_pipeline->get_physical_plans().front();
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

// std::string build_execution_info_message(const std::shared_ptr<SQLPipeline>&
// sql_pipeline) {
//   return sql_pipeline->metrics().to_string();
// }
}  // namespace opossum
