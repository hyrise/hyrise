#pragma once

#include "boost/container/pmr/memory_resource.hpp"
#include "concurrency/transaction_manager.hpp"
#include "scheduler/current_scheduler.hpp"
#include "scheduler/topology.hpp"
#include "storage/storage_manager.hpp"
#include "utils/meta_table_manager.hpp"
#include "utils/plugin_manager.hpp"
#include "utils/singleton.hpp"

namespace opossum {

class JitRepository;
class BenchmarkRunner;

class Hyrise : public Singleton<Hyrise> {
 public:
  // Resets the Hyrise state by deleting its members (e.g., StorageManager) and
  // creating new ones. This is used especially in tests and can lead to a lot of
  // issues if there are still running tasks / threads that want to access a resource.
  // You should be very sure that this is what you want.
  static void reset() {
    Hyrise::get().current_scheduler.get()->finish();
    get() = Hyrise{};
  }

  void set_jit_repository(std::shared_ptr<JitRepository> repo) { jit_repository = repo; }

  void set_benchmark_runner(std::shared_ptr<BenchmarkRunner> runner) { benchmark_runner = runner; }

  PluginManager plugin_manager;
  StorageManager storage_manager;
  TransactionManager transaction_manager;
  MetaTableManager meta_table_manager;
  Topology topology;
  CurrentScheduler current_scheduler;

  std::shared_ptr<JitRepository> jit_repository;
  std::shared_ptr<BenchmarkRunner> benchmark_runner;

 private:
  Hyrise() {
    // The default_memory_resource must be initialized before Hyrise's members so that
    // it is destructed after them and remains accessible during their deconstruction.
    // For example, when the StorageManager is destructed, it causes its stored tables
    // to be deconstructed, too. As these might call deallocate on the
    // default_memory_resource, it is important that the resource has not been
    // destructed before. As objects are destructed in the reverse order of their
    // construction, explicitly initializing the resource first means that it is
    // destructed last.
    boost::container::pmr::get_default_resource();

    plugin_manager = PluginManager{};
    storage_manager = StorageManager{};
    transaction_manager = TransactionManager{};
    meta_table_manager = MetaTableManager{};
    topology = Topology{};
    current_scheduler = CurrentScheduler{};
  }
  friend class Singleton;
};

}  // namespace opossum
