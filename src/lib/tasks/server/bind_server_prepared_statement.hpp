#pragma once

#include "server_task.hpp"

namespace opossum {

class BindServerPreparedStatement : public ServerTask {
 public:
  BindServerPreparedStatement(std::shared_ptr<HyriseSession> session, const std::unique_ptr<SQLPipeline>& sql_pipeline, std::vector<AllParameterVariant> params)
    : ServerTask(std::move(session)), _sql_pipeline(sql_pipeline), _params(std::move(params)) {}

 protected:
  void _on_execute() override;

  const std::unique_ptr<SQLPipeline>& _sql_pipeline;
  std::vector<AllParameterVariant> _params;
};

}  // namespace opossum
