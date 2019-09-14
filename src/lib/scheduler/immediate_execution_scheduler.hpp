#pragma once

#include "abstract_scheduler.hpp"
#include "abstract_task.hpp"

namespace opossum {

/**
 * Instead of actually scheduling, the ImmediateExecutionScheduler executes its tasks immediately.
 */
class ImmediateExecutionScheduler : public AbstractScheduler {
 public:
  void begin() override;

  void wait_for_all_tasks() override;

  void finish() override;

  bool active() const override;

  const std::vector<std::shared_ptr<TaskQueue>>& queues() const override;

  void schedule(std::shared_ptr<AbstractTask> task, NodeID preferred_node_id = CURRENT_NODE_ID,
                SchedulePriority priority = SchedulePriority::Default) override;

 private:
  std::vector<std::shared_ptr<TaskQueue>> _queues = std::vector<std::shared_ptr<TaskQueue>>{};
};

}  // namespace opossum
