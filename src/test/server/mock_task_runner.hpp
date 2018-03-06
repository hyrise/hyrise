#pragma once

#include <boost/thread/future.hpp>

#include <memory>

#include "gmock/gmock.h"

#include "tasks/server/bind_server_prepared_statement_task.hpp"
#include "tasks/server/create_pipeline_task.hpp"
#include "tasks/server/execute_server_prepared_statement_task.hpp"
#include "tasks/server/execute_server_query_task.hpp"
#include "tasks/server/load_server_file_task.hpp"

namespace opossum {

class MockTaskRunner {
 public:
  MOCK_METHOD1(dispatch_server_task,
               boost::future<std::unique_ptr<SQLQueryPlan>>(std::shared_ptr<BindServerPreparedStatementTask>));
  MOCK_METHOD1(dispatch_server_task,
               boost::future<std::unique_ptr<CreatePipelineResult>>(std::shared_ptr<CreatePipelineTask>));
  MOCK_METHOD1(dispatch_server_task,
               boost::future<std::shared_ptr<const Table>>(std::shared_ptr<ExecuteServerPreparedStatementTask>));
  MOCK_METHOD1(dispatch_server_task, boost::future<void>(std::shared_ptr<ExecuteServerQueryTask>));
  MOCK_METHOD1(dispatch_server_task, boost::future<void>(std::shared_ptr<LoadServerFileTask>));
};

}  // namespace opossum
