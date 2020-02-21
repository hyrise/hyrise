#include "hyrise.hpp"

namespace opossum {

Hyrise::Hyrise() {
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
  _scheduler = std::make_shared<ImmediateExecutionScheduler>();
}

void Hyrise::reset() {
  Hyrise::get().scheduler()->finish();
  get() = Hyrise{};
}

const std::shared_ptr<AbstractScheduler>& Hyrise::scheduler() const { return _scheduler; }

void Hyrise::set_scheduler(const std::shared_ptr<AbstractScheduler>& new_scheduler) {
  _scheduler->finish();
  _scheduler = new_scheduler;
  _scheduler->begin();
}

}  // namespace opossum
