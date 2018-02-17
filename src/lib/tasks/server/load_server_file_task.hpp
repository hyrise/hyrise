#pragma once

#include "server_task.hpp"

namespace opossum {

class LoadServerFileTask : public AbstractTask  {
 public:
  LoadServerFileTask(std::string file_name, std::string table_name)
      : _file_name(std::move(file_name)), _table_name(std::move(table_name)) {}

  boost::future<void> get_future() { return _promise.get_future(); }

 protected:
  void _on_execute() override;

  const std::string _file_name;
  const std::string _table_name;

  boost::promise<void> _promise;
};

}  // namespace opossum
