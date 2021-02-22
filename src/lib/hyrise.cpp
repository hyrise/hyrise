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

  transaction_manager = TransactionManager{};
  settings_manager = SettingsManager{};
  log_manager = LogManager{};
  topology = Topology{};
  _scheduler = std::make_shared<ImmediateExecutionScheduler>();
}

void Hyrise::reset() {
  Hyrise::get().scheduler()->finish();
  get() = Hyrise{};
}

const std::shared_ptr<AbstractScheduler>& Hyrise::scheduler() const { return _scheduler; }

bool Hyrise::is_multi_threaded() const {
  return std::dynamic_pointer_cast<ImmediateExecutionScheduler>(_scheduler) == nullptr;
}

void Hyrise::set_scheduler(const std::shared_ptr<AbstractScheduler>& new_scheduler) {
  _scheduler->finish();
  _scheduler = new_scheduler;
  _scheduler->begin();
}

std::shared_ptr<StorageManager> HyriseEnvironmentRef::storage_manager() const {
  auto ret = _storage_manager.lock();
  if (!ret) Fail("Missing storage manager");
  return ret;
}

std::shared_ptr<PluginManager> HyriseEnvironmentRef::plugin_manager() const {
  auto ret = _plugin_manager.lock();
  if (!ret) Fail("Missing plugin manager");
  return ret;
}

std::shared_ptr<MetaTableManager> HyriseEnvironmentRef::meta_table_manager() const {
  auto ret = _meta_table_manager.lock();
  if (!ret) Fail("Missing meta table manager");
  return ret;
}

HyriseEnvironmentRef::HyriseEnvironmentRef() noexcept {}

const std::shared_ptr<HyriseEnvironmentRef>& HyriseEnvironmentHolder::hyrise_env_ref() { return _hyrise_env_ref; }

HyriseEnvironmentHolder::HyriseEnvironmentHolder() {
  _hyrise_env_ref = std::shared_ptr<HyriseEnvironmentRef>(new HyriseEnvironmentRef());
  _storage_manager = std::make_shared<StorageManager>();
  _hyrise_env_ref->_storage_manager = std::weak_ptr<StorageManager>(_storage_manager);
  _plugin_manager = std::make_shared<PluginManager>(_hyrise_env_ref);
  _hyrise_env_ref->_plugin_manager = std::weak_ptr<PluginManager>(_plugin_manager);
  _meta_table_manager = std::make_shared<MetaTableManager>(_hyrise_env_ref);
  _hyrise_env_ref->_meta_table_manager = std::weak_ptr<MetaTableManager>(_meta_table_manager);
}

}  // namespace opossum
