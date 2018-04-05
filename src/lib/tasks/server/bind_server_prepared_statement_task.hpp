#pragma once

#include "abstract_server_task.hpp"

#include "all_parameter_variant.hpp"

namespace opossum {

class SQLPipeline;
class SQLQueryPlan;

// This task is used to bind the actual variables of a prepared statements and return the corresponding query plan.
class BindServerPreparedStatementTask : public AbstractServerTask<std::unique_ptr<SQLQueryPlan>> {
 public:
  BindServerPreparedStatementTask(const SQLPipelineSPtr sql_pipeline,
                                  std::vector<AllParameterVariant> params)
      : _sql_pipeline(sql_pipeline), _params(std::move(params)) {}

 protected:
  void _on_execute() override;

  const SQLPipelineSPtr _sql_pipeline;
  std::vector<AllParameterVariant> _params;
};

}  // namespace opossum
