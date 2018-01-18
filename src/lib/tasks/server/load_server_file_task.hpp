#pragma once

#include "server_task.hpp"

namespace opossum {

class LoadServerFileTask : public ServerTask {
 public:
  LoadServerFileTask(std::shared_ptr<HyriseSession> session, std::string file_name, std::string table_name)
      : ServerTask(std::move(session)), _file_name(std::move(file_name)), _table_name(std::move(table_name)) {}

 protected:
  void _on_execute() override;

  const std::string _file_name;
  const std::string _table_name;
};

}  // namespace opossum
