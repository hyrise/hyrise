#include "execute_server_query_task.hpp"

namespace opossum {

void ExecuteServerQueryTask::_on_execute() {
  try {
    _sql_pipeline.get_result_table();
  } catch (const std::exception& exception) {
    return _session->pipeline_error(exception.what());
  }

  _session->query_executed();
}

}  // namespace opossum
