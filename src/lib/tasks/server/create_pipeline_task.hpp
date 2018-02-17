#pragma once

#include <boost/thread/future.hpp>

#include "server_task.hpp"

namespace opossum {

struct CreatePipelineResult {
  std::shared_ptr<SQLPipeline> sql_pipeline;
  std::optional<std::pair<std::string, std::string>> load_table;
  bool is_load_table;
};

class CreatePipelineTask : public AbstractTask  {
 public:
  CreatePipelineTask(std::string sql) : _sql(sql) {}
  
  boost::future<std::shared_ptr<CreatePipelineResult>> get_future() { return _promise.get_future(); }

 protected:
  void _on_execute() override;

  // This is a slightly hacky way of loading tables via the network interface. We don't support CREATE TABLE yet, so we
  // have to get data into the DB by loading it from a file. If we cannot parse the incoming SQL string, we try to
  // interpret it as a LOAD <file-name> <table-name> command. If this doesn't work, we pass on the parse error.
  bool _is_load_table();

  const std::string _sql;

  std::string _file_name;
  std::string _table_name;
  
  boost::promise<std::shared_ptr<CreatePipelineResult>> _promise;
};

}  // namespace opossum
