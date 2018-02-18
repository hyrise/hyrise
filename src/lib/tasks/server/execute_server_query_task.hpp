#pragma once

#include "server_task.hpp"

namespace opossum {

class ExecuteServerQueryTask : public AbstractTask  {
 public:
  ExecuteServerQueryTask(std::shared_ptr<SQLPipeline> sql_pipeline)
      : _sql_pipeline(sql_pipeline) {}

  boost::future<void> get_future() { return _promise.get_future(); }

 protected:
  void _on_execute() override;

  std::shared_ptr<SQLPipeline> _sql_pipeline;
  boost::promise<void> _promise;
};

}  // namespace opossum
