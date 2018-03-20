#pragma once

#include "abstract_server_task.hpp"

namespace opossum {

// This task is used to load a file from the server's file system (like in Console). This is currently the only way to
// create a table on the server.
class LoadServerFileTask : public AbstractServerTask<void> {
 public:
  LoadServerFileTask(std::string file_name, std::string table_name)
      : _file_name(std::move(file_name)), _table_name(std::move(table_name)) {}

 protected:
  void _on_execute() override;

  const std::string _file_name;
  const std::string _table_name;
};

}  // namespace opossum
