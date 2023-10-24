#include "task_queue.hpp"

#include <execinfo.h>

#include <memory>
#include <utility>

#include "abstract_task.hpp"
#include "types.hpp"
#include "utils/assert.hpp"


namespace { 

void print_predecessors(const auto begin, std::ostream& out, const auto& task, const size_t indent_width = 0) {
  if (task->predecessors().empty()) {
    return;
  }
  
  auto indent = std::string{};
  for (auto i = size_t{0}; i < indent_width; ++i) {
    indent += " ";
  }

  for (const auto& pre_weak : task->predecessors()) {
    const auto pre = pre_weak.lock();
    const auto age = 0;
    out << indent << age << " ms \t " << pre->description() << '\n';
    print_predecessors(begin, out, pre, indent_width + 2);
  } 
}

}  // anonymous namespace

namespace hyrise {

TaskQueue::TaskQueue(NodeID node_id) : _node_id{node_id}, _pull_latencies{} {}

bool TaskQueue::empty() const {
  for (const auto& queue : _queues) {
    if (queue.size_approx() > size_t{0}) {
      return false;
    }
  }
  return true;
}

NodeID TaskQueue::node_id() const {
  return _node_id;
}

void TaskQueue::push(const std::shared_ptr<AbstractTask>& task, const SchedulePriority priority) {
  const auto priority_uint = static_cast<uint32_t>(priority);
  DebugAssert(priority_uint < NUM_PRIORITY_LEVELS, "Illegal priority level");

  // Someone else was first to enqueue this task? No problem!
  if (!task->try_mark_as_enqueued()) {
    return;
  }
  
  //std::printf("%p\n", (void*)&*this);

  Assert(_node_id == NodeID{1}, "Unexpected NodeID: " + std::to_string(static_cast<size_t>(_node_id)));

  //std::cout << task->description() << std::endl;
  //std::cout << _node_id << " is node_id " << std::endl;
  task->set_node_id(_node_id);
  [[maybe_unused]] const auto enqueue_successful = _queues[priority_uint].enqueue(task);
  //const auto queue_size = _queues[priority_uint].size_approx();
  //if (queue_size > 2000 && queue_size % 1018 == 0) 
  //  std::printf("size of queue: %zu\n", queue_size);
  DebugAssert(enqueue_successful, "Enqueuing did not succeed.");
  semaphore.signal();
}

std::shared_ptr<AbstractTask> TaskQueue::pull() {
  auto task = std::shared_ptr<AbstractTask>{};
  for (auto& queue : _queues) {
    if (queue.try_dequeue(task)) {
      std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now();
      const auto age = static_cast<size_t>(std::chrono::duration_cast<std::chrono::microseconds>(now - task->tp).count());
      
      const auto unix_timestamp_us = static_cast<size_t>(std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count());
      const auto unix_timestamp_s = static_cast<size_t>(std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count());
      if (age > 0) {
        const auto latency_power = static_cast<size_t>(std::log10(age));
        Assert(latency_power < 11, "NARF: " + std::to_string(latency_power));
        ++_pull_latencies[latency_power];
      }

      if (unix_timestamp_us % 10000 == 0 && unix_timestamp_s % 5 == 0) {// && task->id() % 1717 == 0) {
        auto ss_1 = std::stringstream{};
        auto ss_2 = std::stringstream{};
        for (auto i = int32_t{11}; i >= 0; --i) {
          const auto latency = _pull_latencies[i];
          ss_1 << i << " - ";
          ss_2 << latency << " - ";
        }
        ss_1 << '\n' << ss_2.str() << '\n';
        std::cout << ss_1.str();
      }
      
      if (age > 100'000'000) {  // older than 10s.
        auto ss = std::stringstream{};
        ss << "Current size of queues >> high: " << _queues[0].size_approx() << ", low: " << _queues[1].size_approx() << '\n';
        ss << "Task's age: " << age / 1'000 << " ms >> " << task->description() << '\n';
        ss << "Task's predecessors/successors/: #" << task->predecessors().size() << " & #" << task->successors().size() << '\n';
        print_predecessors(now, ss, task);
        /*
        if (!task->predecessors().empty()) {
          for (const auto&  pre_weak : task->predecessors()) {
            const auto pre = pre_weak.lock();
            const auto pre_age = static_cast<size_t>(std::chrono::duration_cast<std::chrono::microseconds>(now - pre->tp).count());
            ss << "Predecessor age: " << pre_age / 1'000 << " ms >> " << pre->description() << '\n';
          }
        }
        */

        const int size = 20;
        void *buffer[size];

        // get the void* pointers to functions; "ret" is the number of items returned
        int ret = backtrace( buffer, size );

        // now we want to convert those pointers to text
        char **ptr = backtrace_symbols( buffer, ret );

	      for (int idx = 0; idx < ret; idx ++)  {
		     // if using C++, look up abi::__cxa_demangle( ... ) in #include <cxxabi.h>
      		ss << '\t' << idx << ": " << ptr[idx] << '\n';
	      }
        // ... do something here

        // once done, remember to de-allocate ptr
        free(ptr);
        std::cout << ss.str();
      }
      return task;
    }
  }

  // We waited for the semaphore to enter pull() but did not receive a task. Ensure that queues are checked again.
  semaphore.signal();
  return nullptr;
}

std::shared_ptr<AbstractTask> TaskQueue::steal() {
  auto task = std::shared_ptr<AbstractTask>{};
  for (auto& queue : _queues) {
    if (queue.try_dequeue(task)) {
      if (task->is_stealable()) {
        return task;
      }

      [[maybe_unused]] const auto enqueue_successful = queue.enqueue(task);
      DebugAssert(enqueue_successful, "Enqueuing stolen task did not succeed.");
      semaphore.signal();
    }
  }

  // We waited for the semaphore to enter steal() but did not receive a task. Ensure that queues are checked again.
  semaphore.signal();
  return nullptr;
}

size_t TaskQueue::estimate_load() const {
  auto estimated_load = size_t{0};

  // Simple heuristic to estimate the load: the higher the priority, the higher the costs. We use powers of two
  // (starting with 2^0) to calculate the cost factor per priority level.
  for (auto queue_id = size_t{0}; queue_id < NUM_PRIORITY_LEVELS; ++queue_id) {
    // The lowest priority has a multiplier of 2^0, the next higher priority 2^1, and so on.
    estimated_load += _queues[queue_id].size_approx() * (size_t{1} << (NUM_PRIORITY_LEVELS - 1 - queue_id));
  }

  return estimated_load;
}

void TaskQueue::signal(const size_t count) {
  semaphore.signal(count);
}

}  // namespace hyrise
