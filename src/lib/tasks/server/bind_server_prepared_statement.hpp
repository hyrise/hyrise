#pragma once

#include "server_task.hpp"

#include "all_parameter_variant.hpp"

namespace opossum {

class SQLPipeline;

class BindServerPreparedStatement : public ServerTask<void> {
 public:
  BindServerPreparedStatement(const std::unique_ptr<SQLPipeline>& sql_pipeline,
                              std::vector<AllParameterVariant> params)
      : _sql_pipeline(sql_pipeline), _params(std::move(params)) {}

 protected:
  void _on_execute() override;

  const std::unique_ptr<SQLPipeline>& _sql_pipeline;
  std::vector<AllParameterVariant> _params;
};

}  // namespace opossum
