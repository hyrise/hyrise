#pragma once

#include <boost/thread/future.hpp>

#include "abstract_server_task.hpp"

namespace opossum {

class SQLPipeline;

struct CreatePipelineResult {
  std::shared_ptr<SQLPipeline> sql_pipeline;
  std::optional<std::pair<std::string, std::string>> load_table;
};

// This task is used to parse an SQL string from a client and wrap it in an SQLPipeline. It is a separate task and not
// "inlined" because parsing can be a potentially expensive task on long strings and we want to reduce the computational
// load on the main server thread to a miminum.
class CreatePipelineTask : public AbstractServerTask<std::unique_ptr<CreatePipelineResult>> {
 public:
  explicit CreatePipelineTask(std::string sql, bool allow_load_table = false)
      : _sql(sql), _allow_load_table(allow_load_table) {}

 protected:
  void _on_execute() override;

  // This is a slightly hacky way of loading tables via the network interface. We don't support CREATE TABLE yet, so we
  // have to get data into the DB by loading it from a file. If we cannot parse the incoming SQL string, we try to
  // interpret it as a LOAD <file-name> <table-name> command. If this doesn't work, we pass on the parse error.
  bool _is_load_table();

  const std::string _sql;
  const bool _allow_load_table;

  std::string _file_name;
  std::string _table_name;
};

}  // namespace opossum
