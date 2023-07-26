#pragma once

#include "abstract_task.hpp"

namespace hyrise {

/**
 * TODO:
 */
class ShutdownTask : public AbstractTask {
 public:
  explicit ShutdownTask(std::atomic_uint64_t& active_worker_count)
      : AbstractTask{SchedulePriority::Default, false}, _active_worker_count{active_worker_count} {}

  bool is_shutdown_task() const;

 protected:
  void _on_execute() override;

 private:
  std::atomic_uint64_t& _active_worker_count;
};
}  // namespace hyrise
