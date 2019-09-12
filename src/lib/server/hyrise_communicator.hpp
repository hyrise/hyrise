#pragma once


#include "storage/table.hpp"

#include "hyrise.hpp"
#include "postgres_handler.hpp"
// #include "scheduler/abstract_task.hpp"
#include "operators/abstract_operator.hpp"

#include "types.hpp"

namespace opossum {

class HyriseCommunicator {
 public:
  static uint64_t send_query_response(std::shared_ptr<const Table> table, std::shared_ptr<PostgresHandler> postgres_handler);

  static std::pair<std::shared_ptr<const Table>, OperatorType> execute_pipeline(const std::string& sql);

  static void setup_prepared_plan(const std::string& statement_name, const std::string& query);

  static std::shared_ptr<AbstractOperator> bind_prepared_plan(const PreparedStatementDetails& statement_details);

  static std::shared_ptr<TransactionContext> get_new_transaction_context();

  static std::shared_ptr<const Table> execute_prepared_statement(std::shared_ptr<AbstractOperator> physical_plan);
  };

}  // namespace opossum
