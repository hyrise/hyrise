#pragma once

#include "server_task.hpp"
#include "storage/table.hpp"

namespace opossum {

class SendQueryResponseTask : public ServerTask {
 public:
  SendQueryResponseTask(std::shared_ptr<HyriseSession> session, const std::shared_ptr<const Table>& result_table)
      : ServerTask(std::move(session)), _result_table(result_table) {}

 protected:
  void _on_execute() override;

  void _send_row_description();
  void _send_row_data();
  void _send_command_complete();

  const std::shared_ptr<const Table>& _result_table;
  uint64_t _row_count = 0;
};

}  // namespace opossum
