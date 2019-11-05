#pragma once

#include "boost/container/pmr/memory_resource.hpp"
#include "concurrency/transaction_manager.hpp"
#include "scheduler/immediate_execution_scheduler.hpp"
#include "scheduler/topology.hpp"
#include "storage/storage_manager.hpp"
#include "utils/meta_table_manager.hpp"
#include "utils/plugin_manager.hpp"
#include "utils/singleton.hpp"

namespace opossum {

class AbstractScheduler;
class BenchmarkRunner;

// This should be the only singleton in the src/lib world. It provides a unified way of accessing components like the
// storage manager, the transaction manager, and more. Encapsulating this in one class avoids the static initialization
// order fiasco, which would otherwise make the initialization/destruction order hard to control.
class Hyrise : public Singleton<Hyrise> {
 public:
  // Resets the Hyrise state by deleting its members (e.g., StorageManager) and
  // creating new ones. This is used especially in tests and can lead to a lot of
  // issues if there are still running tasks / threads that want to access a resource.
  // You should be very sure that this is what you want.
  static void reset();

  const std::shared_ptr<AbstractScheduler>& scheduler() const;

  void set_scheduler(const std::shared_ptr<AbstractScheduler>& new_scheduler);

  PluginManager plugin_manager;
  StorageManager storage_manager;
  TransactionManager transaction_manager;
  MetaTableManager meta_table_manager;
  Topology topology;

  // The BenchmarkRunner is available here so that non-benchmark components can add information to the benchmark
  // result JSON.
  std::weak_ptr<BenchmarkRunner> benchmark_runner;

 private:
  Hyrise();
  friend class Singleton;

  // (Re-)setting the scheduler requires more than just replacing the pointer. To make sure that set_scheduler is used,
  // the scheduler is private.
  std::shared_ptr<AbstractScheduler> _scheduler;
};

}  // namespace opossum
