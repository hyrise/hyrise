#pragma once

#include "abstract_server_task.hpp"

#include "all_parameter_variant.hpp"

namespace opossum {

class SQLPipeline;
class SQLQueryPlan;

// This task is used to bind the actual variables of a prepared statements and return the corresponding query plan.
class BindServerPreparedStatementTask : public AbstractServerTask<std::unique_ptr<SQLQueryPlan>> {
 public:
  BindServerPreparedStatementTask(const std::shared_ptr<SQLPipeline> sql_pipeline, std::vector<AllTypeVariant> params)
      : _sql_pipeline(sql_pipeline), _params(std::move(params)) {}

 protected:
  void _on_execute() override;

  const std::shared_ptr<SQLPipeline> _sql_pipeline;
  std::vector<AllTypeVariant> _params;
};

}  // namespace opossum
