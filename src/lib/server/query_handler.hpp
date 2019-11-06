#pragma once

#include <variant>
#include "hyrise.hpp"
#include "operators/abstract_operator.hpp"
#include "postgres_protocol_handler.hpp"
#include "storage/table.hpp"

namespace opossum {

// Store relevant information after pipeline execution
struct ExecutionInformation {
  std::shared_ptr<const Table> result_table;
  // Since the PostgreSQL Wire Protocol requires the query type (such as SELECT, INSERT, UPDATE,...) we need to store
  // the root operator's type.
  OperatorType root_operator;
  std::string pipeline_metrics;
  ErrorMessage error_message;
};

// This class manages the interaction between the server and the database component. Furthermore, most of the SQL-based
// error handling happens in this class.
class QueryHandler {
 public:
  static ExecutionInformation execute_pipeline(const std::string& query, const SendExecutionInfo send_execution_info);

  static void setup_prepared_plan(const std::string& statement_name, const std::string& query);

  static std::shared_ptr<AbstractOperator> bind_prepared_plan(const PreparedStatementDetails& statement_details);

  static std::shared_ptr<const Table> execute_prepared_plan(const std::shared_ptr<AbstractOperator>& physical_plan);
};

}  // namespace opossum
