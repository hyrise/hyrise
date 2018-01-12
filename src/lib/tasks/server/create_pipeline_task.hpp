#pragma once

#include "server_task.hpp"

namespace opossum {

class CreatePipelineTask : public ServerTask {
 public:
  CreatePipelineTask(std::shared_ptr<HyriseSession> session, const std::string& sql)
      : ServerTask(std::move(session)), _sql(sql) {}

 protected:
  void _on_execute() override;

  const std::string _sql;
};

}  // namespace opossum
