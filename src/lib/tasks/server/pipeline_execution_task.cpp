#include "pipeline_execution_task.hpp"
#include <future>
#include "sql/sql_pipeline_builder.hpp"

namespace opossum {

PipelineExecutionTask::PipelineExecutionTask(const std::string& sql) : _sql(sql) {}

void PipelineExecutionTask::_on_execute() {
  _sql_pipeline = std::make_shared<SQLPipeline>(SQLPipelineBuilder{_sql}.create_pipeline());
  // _sql_pipeline->get_result_table();

  // Async here, because otherwise task might be scheduled to same thread
  std::async(std::launch::deferred, [&]{ _sql_pipeline->get_result_table(); });
  
}

std::shared_ptr<SQLPipeline> PipelineExecutionTask::get_sql_pipeline() const {
    while (!_sql_pipeline) {}
    return _sql_pipeline;
}


}  // namespace opossum
