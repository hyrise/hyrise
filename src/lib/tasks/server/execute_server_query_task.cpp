#include "execute_server_query_task.hpp"

namespace opossum {

void ExecuteServerQueryTask::_on_execute() {
  try {
    _sql_pipeline.get_result_table();
    _promise.set_value();
  } catch (const std::exception& exception) {
    _promise.set_exception(exception);
  }
}

}  // namespace opossum
