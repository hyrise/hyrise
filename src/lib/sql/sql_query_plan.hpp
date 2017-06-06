
#include "scheduler/operator_task.hpp"

namespace opossum {

class SQLQueryPlan {
 public:
  SQLQueryPlan();

  // Returns the current size of the query plan.
  size_t size() const;

  // Returns the task that was most recently added to the plan.
  std::shared_ptr<OperatorTask> back();

  // Adds a task to the query plan.
  void add(std::shared_ptr<OperatorTask> task);

  // Remove all tasks from the current plan.
  void clear();

  const std::vector<std::shared_ptr<OperatorTask>>& tasks() const;

 protected:
  std::vector<std::shared_ptr<OperatorTask>> _tasks;
};

}  // namespace opossum
