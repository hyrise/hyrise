#pragma once

#include "server_task.hpp"
#include "storage/table.hpp"

namespace opossum {

class SendQueryResponseTask : public ServerTask {
 public:
  SendQueryResponseTask(std::shared_ptr<HyriseSession> session, SQLPipeline& sql_pipeline,
                        std::shared_ptr<const Table> explicit_result_table)
      : ServerTask(std::move(session)), _sql_pipeline(sql_pipeline), _result_table(std::move(explicit_result_table)) {}

 protected:
  void _on_execute() override;

  void _send_row_description();
  void _send_row_data();
  void _send_command_complete();
  void _send_execution_info();

  SQLPipeline& _sql_pipeline;
  const std::shared_ptr<const Table> _result_table;
  uint64_t _row_count = 0;
};

}  // namespace opossum
