#pragma once

#include "abstract_server_task.hpp"

namespace opossum {

// This task is used to recover from logfiles (like in Console).
class RecoverDatabaseTask : public AbstractServerTask<void> {
 public:
  RecoverDatabaseTask() {}

 protected:
  void _on_execute() override;
};

}  // namespace opossum
