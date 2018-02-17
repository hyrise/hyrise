#pragma once

#include "scheduler/abstract_task.hpp"
#include "server/hyrise_session.hpp"

namespace opossum {

class ServerTask : public AbstractTask {
 // TODO: Re-purpose this abstract class to manage the result promise
 public:
  explicit ServerTask(std::shared_ptr<HyriseSession> session) : _session(std::move(session)) {}

 protected:
  std::shared_ptr<HyriseSession> _session;
};

}  // namespace opossum
