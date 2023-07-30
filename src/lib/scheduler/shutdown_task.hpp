#pragma once

#include "abstract_task.hpp"

namespace hyrise {

/**
 * ShutdownTasks are used to signal workers that the NodeQueueScheduler is going to shut down. The actual task only
 * decrements the number of active workers, which needs to be passed during construction.
 */
class ShutdownTask : public AbstractTask {
 public:
  explicit ShutdownTask(std::atomic_int64_t& active_worker_count)
      : AbstractTask{SchedulePriority::Default, false}, _active_worker_count{active_worker_count} {}

 protected:
  void _on_execute() override;

 private:
  std::atomic_int64_t& _active_worker_count;
};
}  // namespace hyrise
