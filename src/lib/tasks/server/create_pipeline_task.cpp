#include "create_pipeline_task.hpp"

#include "sql/sql_pipeline.hpp"

namespace opossum {

void CreatePipelineTask::_on_execute() {
  std::unique_ptr<SQLPipeline> sql_pipeline;
  try {
    sql_pipeline = std::make_unique<SQLPipeline>(_sql);
  } catch (const std::exception& exception) {
    return _session->pipeline_error(exception.what());
  }

  _session->pipeline_created(std::move(sql_pipeline));
}

}  // namespace opossum
