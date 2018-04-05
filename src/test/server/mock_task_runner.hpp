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
               boost::future<std::unique_ptr<SQLQueryPlan>>(BindServerPreparedStatementTaskSPtr));
  MOCK_METHOD1(dispatch_server_task,
               boost::future<std::unique_ptr<CreatePipelineResult>>(CreatePipelineTaskSPtr));
  MOCK_METHOD1(dispatch_server_task,
               boost::future<TableCSPtr>(ExecuteServerPreparedStatementTaskSPtr));
  MOCK_METHOD1(dispatch_server_task, boost::future<void>(ExecuteServerQueryTaskSPtr));
  MOCK_METHOD1(dispatch_server_task, boost::future<void>(LoadServerFileTaskSPtr));
};

}  // namespace opossum
