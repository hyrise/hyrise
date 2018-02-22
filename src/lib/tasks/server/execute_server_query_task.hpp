#pragma once

#include "server_task.hpp"

namespace opossum {

class SQLPipeline;

class ExecuteServerQueryTask : public ServerTask<void> {
 public:
  explicit ExecuteServerQueryTask(std::shared_ptr<SQLPipeline> sql_pipeline) : _sql_pipeline(sql_pipeline) {}

 protected:
  void _on_execute() override;

  std::shared_ptr<SQLPipeline> _sql_pipeline;
};

}  // namespace opossum
