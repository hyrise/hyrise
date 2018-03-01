#include "execute_server_query_task.hpp"

#include "sql/sql_pipeline.hpp"

namespace opossum {

void ExecuteServerQueryTask::_on_execute() {
  try {
    _sql_pipeline->get_result_table();
    // Indicate that execution is finished. The result is accessed from outside so we need an empty promise.
    _promise.set_value();
  } catch (const std::exception& exception) {
    _promise.set_exception(boost::current_exception());
  }
}

}  // namespace opossum
