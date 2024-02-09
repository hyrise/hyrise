#include "hyrise.hpp"

#include <memory>

#include <boost/container/pmr/memory_resource.hpp>

#include "concurrency/transaction_manager.hpp"
#include "scheduler/abstract_scheduler.hpp"
#include "scheduler/immediate_execution_scheduler.hpp"
#include "scheduler/topology.hpp"
#include "storage/storage_manager.hpp"
#include "utils/log_manager.hpp"
#include "utils/meta_table_manager.hpp"
#include "utils/plugin_manager.hpp"
#include "utils/settings_manager.hpp"

namespace hyrise {

Hyrise::Hyrise() {
  // The default_memory_resource must be initialized before Hyrise's members so that it is destructed after them and
  // remains accessible during their deconstruction. For example, when the StorageManager is destructed, it causes its
  // stored tables to be deconstructed, too. As these might call deallocate on the default_memory_resource, it is
  // important that the resource has not been destructed before. As objects are destructed in the reverse order of their
  // construction, explicitly initializing the resource first means that it is destructed last.
  boost::container::pmr::get_default_resource();

  storage_manager = StorageManager{};
  plugin_manager = PluginManager{};
  transaction_manager = TransactionManager{};
  meta_table_manager = MetaTableManager{};
  settings_manager = SettingsManager{};
  log_manager = LogManager{};
  topology = Topology{};
  _scheduler = std::make_shared<ImmediateExecutionScheduler>();
}

void Hyrise::reset() {
  Hyrise::get().scheduler()->finish();
  get() = Hyrise{};
}

const std::shared_ptr<AbstractScheduler>& Hyrise::scheduler() const {
  return _scheduler;
}

bool Hyrise::is_multi_threaded() const {
  return std::dynamic_pointer_cast<ImmediateExecutionScheduler>(_scheduler) == nullptr;
}

void Hyrise::set_scheduler(const std::shared_ptr<AbstractScheduler>& new_scheduler) {
  _scheduler->finish();
  _scheduler = new_scheduler;
  _scheduler->begin();
}

}  // namespace hyrise
