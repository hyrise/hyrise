#pragma once

#include "abstract_server_task.hpp"

namespace opossum {

class SQLPipeline;

// This task is used in the SimpleQueryCommand mode where we have a simple pipeline that needs to be executed.
class ExecuteServerQueryTask : public AbstractServerTask<void> {
 public:
  explicit ExecuteServerQueryTask(std::shared_ptr<SQLPipeline> sql_pipeline) : _sql_pipeline(sql_pipeline) {}

 protected:
  void _on_execute() override;

  std::shared_ptr<SQLPipeline> _sql_pipeline;
};

}  // namespace opossum
