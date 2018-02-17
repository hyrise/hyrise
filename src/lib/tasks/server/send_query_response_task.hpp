#pragma once

#include "server_task.hpp"
#include "storage/table.hpp"

namespace opossum {

class SendQueryResponseTask : public AbstractTask  {
 public:
  SendQueryResponseTask(std::shared_ptr<ClientConnection> connection, SQLPipeline& sql_pipeline,
                        std::shared_ptr<const Table> explicit_result_table)
      : _connection(connection), _sql_pipeline(sql_pipeline), _result_table(std::move(explicit_result_table)) {}

  boost::future<void> get_future() { return _promise.get_future(); }

 protected:
  void _on_execute() override;

  // TODO: Move these to ClientConnection
  void _send_row_description();
  void _send_row_data();
  void _send_command_complete();
  void _send_execution_info();

  std::shared_ptr<ClientConnection> _connection;
  SQLPipeline& _sql_pipeline;
  const std::shared_ptr<const Table> _result_table;
  uint64_t _row_count = 0;
  
  boost::promise<void> _promise;
};

}  // namespace opossum
