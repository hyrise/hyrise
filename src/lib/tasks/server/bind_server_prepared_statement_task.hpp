#pragma once

#include "server_task.hpp"

#include "all_parameter_variant.hpp"

namespace opossum {

class SQLPipeline;
class SQLQueryPlan;

class BindServerPreparedStatementTask : public ServerTask<std::unique_ptr<SQLQueryPlan>> {
 public:
  BindServerPreparedStatementTask(const std::shared_ptr<SQLPipeline> sql_pipeline,
                                  std::vector<AllParameterVariant> params)
      : _sql_pipeline(sql_pipeline), _params(std::move(params)) {}

 protected:
  void _on_execute() override;

  const std::shared_ptr<SQLPipeline> _sql_pipeline;
  std::vector<AllParameterVariant> _params;
};

}  // namespace opossum
