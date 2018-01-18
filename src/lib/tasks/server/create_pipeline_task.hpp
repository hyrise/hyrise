#pragma once

#include "server_task.hpp"

namespace opossum {

class CreatePipelineTask : public ServerTask {
 public:
  CreatePipelineTask(std::shared_ptr<HyriseSession> session, std::string sql)
      : ServerTask(std::move(session)), _sql(std::move(sql)) {}

 protected:
  void _on_execute() override;

  // This is a slightly hacky way of loading tables via the network interface. We don't support CREATE TABLE yet, so we
  // have to get data into the DB by loading it from a file. If we cannot parse the incoming SQL string, we try to
  // interpret it as a LOAD <file-name> <table-name> command. If this doesn't work, we pass on the parse error.
  bool _is_load_table();

  const std::string _sql;

  std::string _file_name;
  std::string _table_name;
};

}  // namespace opossum
