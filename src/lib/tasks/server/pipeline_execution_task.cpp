#include "pipeline_execution_task.hpp"

#include "sql/sql_pipeline_builder.hpp"

namespace opossum {

PipelineExecutionTask::PipelineExecutionTask(const std::string& sql) : _sql(sql) {}

void PipelineExecutionTask::_on_execute() {
  _sql_pipeline = std::make_shared<SQLPipeline>(SQLPipelineBuilder{_sql}.create_pipeline());
  _sql_pipeline->get_result_table();
}

std::shared_ptr<SQLPipeline> PipelineExecutionTask::get_sql_pipeline() const {
    return _sql_pipeline;
}


}  // namespace opossum
