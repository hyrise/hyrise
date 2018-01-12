#pragma once

#include "server_task.hpp"

namespace opossum {

class ExecuteServerQueryTask : public ServerTask {
 public:
  ExecuteServerQueryTask(std::shared_ptr<HyriseSession> session, SQLPipeline& sql_pipeline)
      : ServerTask(std::move(session)), _sql_pipeline(sql_pipeline) {}

 protected:
  void _on_execute() override;

  SQLPipeline& _sql_pipeline;
};

}  // namespace opossum
