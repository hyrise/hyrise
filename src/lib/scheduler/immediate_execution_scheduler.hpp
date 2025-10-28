#pragma once

#include <memory>
#include <vector>

#include "abstract_scheduler.hpp"
#include "abstract_task.hpp"

namespace hyrise {

/**
 * Instead of actually scheduling, the ImmediateExecutionScheduler executes its tasks immediately.
 */
class ImmediateExecutionScheduler : public AbstractScheduler {
 public:
  void begin() final;

  void wait_for_all_tasks() final;

  void finish() final;

  bool active() const final;

  const std::vector<std::shared_ptr<TaskQueue>>& queues() const final;

 private:
  void _schedule(std::shared_ptr<AbstractTask> task, NodeID preferred_node_id = CURRENT_NODE_ID,
                 SchedulePriority priority = SchedulePriority::Default) final;

  void _group_tasks(const std::vector<std::shared_ptr<AbstractTask>>& tasks) const final;

  std::vector<std::shared_ptr<TaskQueue>> _queues = std::vector<std::shared_ptr<TaskQueue>>{};
};

}  // namespace hyrise
