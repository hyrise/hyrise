#pragma once

#include "hyrise.hpp"
#include "operators/abstract_operator.hpp"
#include "postgres_protocol_handler.hpp"
#include "storage/table.hpp"

namespace opossum {

// This class manages the interaction between the server and the database component.
class HyriseCommunicator {
 public:
  static std::pair<std::shared_ptr<const Table>, OperatorType> execute_pipeline(const std::string& sql);

  static void setup_prepared_plan(const std::string& statement_name, const std::string& query);

  static std::shared_ptr<AbstractOperator> bind_prepared_plan(const PreparedStatementDetails& statement_details);

  static std::shared_ptr<TransactionContext> get_new_transaction_context();

  static std::shared_ptr<const Table> execute_prepared_statement(const std::shared_ptr<AbstractOperator>& physical_plan);
};

}  // namespace opossum
