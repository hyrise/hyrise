#pragma once

namespace opossum {

class MockTaskRunner {
 public:
  MOCK_METHOD1(dispatch_server_task,
               boost::future<std::unique_ptr<PreparedPlan>>(std::shared_ptr<ParseServerPreparedStatementTask>));
  MOCK_METHOD1(dispatch_server_task,
               boost::future<std::shared_ptr<AbstractOperator>>(std::shared_ptr<BindServerPreparedStatementTask>));
  MOCK_METHOD1(dispatch_server_task,
               boost::future<std::unique_ptr<CreatePipelineResult>>(std::shared_ptr<CreatePipelineTask>));
  MOCK_METHOD1(dispatch_server_task,
               boost::future<std::shared_ptr<const Table>>(std::shared_ptr<ExecuteServerPreparedStatementTask>));
  MOCK_METHOD1(dispatch_server_task, boost::future<void>(std::shared_ptr<ExecuteServerQueryTask>));
  MOCK_METHOD1(dispatch_server_task, boost::future<void>(std::shared_ptr<LoadServerFileTask>));
};

}  // namespace opossum
