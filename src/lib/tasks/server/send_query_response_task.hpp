#pragma once

#include "sql/SQLStatement.h"

#include "server/client_connection.hpp"
#include "storage/table.hpp"

#include "server_task.hpp"

namespace opossum {

class SQLPipeline;

class SendQueryResponseTask : public ServerTask<uint64_t> {
 public:
  SendQueryResponseTask(std::shared_ptr<ClientConnection> connection, std::shared_ptr<const Table> result_table)
      : _connection(connection), _result_table(std::move(result_table)) {}

  static std::vector<ColumnDescription> build_row_description(const std::shared_ptr<const Table> table);
  static std::string build_command_complete_message(hsql::StatementType statement_type, uint64_t row_count);
  static std::string build_execution_info_message(std::shared_ptr<SQLPipeline> sql_pipeline);

 protected:
  void _on_execute() override;

  std::shared_ptr<ClientConnection> _connection;
  const std::shared_ptr<const Table> _result_table;
};

}  // namespace opossum
