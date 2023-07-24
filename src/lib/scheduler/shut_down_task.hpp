#pragma once

#include "abstract_task.hpp"

namespace hyrise {

/**
 * TODO:
 */
class ShutDownTask : public AbstractTask {
 public:
  explicit ShutDownTask(std::atomic_uint64_t& active_worker_count)
      : AbstractTask{TaskType::ShutDownTask, SchedulePriority::Default, false},
        _active_worker_count{active_worker_count} {}

 protected:
  void _on_execute() override;

 private:
  std::atomic_uint64_t& _active_worker_count;
};
}  // namespace hyrise
