#pragma once

#include "sql/sql_pipeline.hpp"

namespace opossum {

class PipelineExecutionTask : public AbstractTask {
 public:
  explicit PipelineExecutionTask(const std::string& sql);

  std::shared_ptr<SQLPipeline> get_sql_pipeline() const;

 protected:
  void _on_execute() override;

 private:
  const std::string _sql;
  std::shared_ptr<SQLPipeline> _sql_pipeline;
};
}  // namespace opossum
