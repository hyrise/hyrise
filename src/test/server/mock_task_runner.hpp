#pragma once

#include <memory>

#include <gmock/gmock.h>
#include <boost/thread/future.hpp>

#include "tasks/server/bind_server_prepared_statement_task.hpp"
#include "tasks/server/commit_transaction_task.hpp"
#include "tasks/server/create_pipeline_task.hpp"
#include "tasks/server/execute_server_prepared_statement_task.hpp"
#include "tasks/server/execute_server_query_task.hpp"
#include "tasks/server/load_server_file_task.hpp"
#include "tasks/server/send_query_response_task.hpp"

namespace opossum {

class MockTaskRunner {
 public:
  MOCK_METHOD1(dispatch_server_task,
               boost::future<std::unique_ptr<SQLQueryPlan>>(std::shared_ptr<BindServerPreparedStatementTask>));
  MOCK_METHOD1(dispatch_server_task, boost::future<void>(std::shared_ptr<CommitTransactionTask>));
  MOCK_METHOD1(dispatch_server_task,
               boost::future<std::unique_ptr<CreatePipelineResult>>(std::shared_ptr<CreatePipelineTask>));
  MOCK_METHOD1(dispatch_server_task,
               boost::future<std::shared_ptr<const Table>>(std::shared_ptr<ExecuteServerPreparedStatementTask>));
  MOCK_METHOD1(dispatch_server_task, boost::future<void>(std::shared_ptr<ExecuteServerQueryTask>));
  MOCK_METHOD1(dispatch_server_task, boost::future<void>(std::shared_ptr<LoadServerFileTask>));
  MOCK_METHOD1(dispatch_server_task, boost::future<uint64_t>(std::shared_ptr<SendQueryResponseTask>));
};

}  // namespace opossum
