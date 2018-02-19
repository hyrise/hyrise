#pragma once

#include "sql/SQLStatement.h"

#include "server_task.hpp"
#include "storage/table.hpp"

namespace opossum {

class ClientConnection;
class SQLPipeline;

class SendQueryResponseTask : public ServerTask<void>  {
 public:
  SendQueryResponseTask(std::shared_ptr<ClientConnection> connection, hsql::StatementType statement_type,
                        std::shared_ptr<SQLPipeline> sql_pipeline,
                        std::shared_ptr<const Table> explicit_result_table)
      : _connection(connection), _statement_type(statement_type),
        _sql_pipeline(sql_pipeline), _result_table(std::move(explicit_result_table)) {}

 protected:
  void _on_execute() override;

  void _send_row_description();
  void _send_row_data();
  void _send_command_complete();
  void _send_execution_info();

  std::shared_ptr<ClientConnection> _connection;
  hsql::StatementType _statement_type;
  std::shared_ptr<SQLPipeline> _sql_pipeline;
  const std::shared_ptr<const Table> _result_table;
  uint64_t _row_count = 0;
};

}  // namespace opossum
