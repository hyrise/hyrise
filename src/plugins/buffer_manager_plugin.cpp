#include "buffer_manager_plugin.hpp"

#include "storage/table.hpp"

namespace hyrise {

std::string BufferManagerPlugin::description() const {
  return "Hyrise Buffer Manager Plugin that allows the runtime configuration of the buffer manager.";
}

void BufferManagerPlugin::start() {
  buffer_manager_pool_size_setting = std::make_shared<BufferManagerPoolSizeSetting>();
  buffer_manager_pool_size_setting->register_at_settings_manager();

  buffer_manager_ssd_path_setting = std::make_shared<BufferManagerSSDPathSetting>();
  buffer_manager_ssd_path_setting->register_at_settings_manager();
}

void BufferManagerPlugin::stop() {
  buffer_manager_pool_size_setting->unregister_at_settings_manager();
  buffer_manager_ssd_path_setting->unregister_at_settings_manager();
}

EXPORT_PLUGIN(BufferManagerPlugin);

BufferManagerPlugin::BufferManagerSetting::BufferManagerSetting(BufferManager& buffer_manager, const std::string& name)
    : AbstractSetting("BufferManager." + std::to_string(buffer_manager.buffer_pools()[0]->get_node_id()) + "." + name),
      buffer_manager(buffer_manager) {}

BufferManagerPlugin::BufferManagerPoolSizeSetting::BufferManagerPoolSizeSetting()
    : BufferManagerSetting(Hyrise::get().buffer_manager, "buffer_pool_size") {
  // TODO: Pass topology to BufferManagerSetting
  //   buffer_pool_size = Hyrise::get().topology.total_bytes(BufferManagerSetting::node_id) * 8 / 10
}

const std::string& BufferManagerPlugin::BufferManagerPoolSizeSetting::description() const {
  static const auto description = std::string{"Size of the buffer pool in bytes"};
  return description;
}

const std::string& BufferManagerPlugin::BufferManagerPoolSizeSetting::get() {
  pool_size = std::to_string(buffer_manager.buffer_pools()[0]->get_max_bytes());
  return pool_size;
}

void BufferManagerPlugin::BufferManagerPoolSizeSetting::set(const std::string& value) {
  pool_size = value;
  const auto size = std::stoull(value);
  buffer_manager.buffer_pools()[0]->resize(size);
}

BufferManagerPlugin::BufferManagerSSDPathSetting::BufferManagerSSDPathSetting()
    : BufferManagerSetting(Hyrise::get().buffer_manager, "ssd_path"),
      // TODO: Move
      ssd_path(std::filesystem::current_path() / "buffer_manager_data") {
  if (std::filesystem::exists(ssd_path) || std::filesystem::create_directory(ssd_path)) {
    return;
  }
  Fail("Failed to create buffer manager data directory: " + ssd_path);
}

const std::string& BufferManagerPlugin::BufferManagerSSDPathSetting::description() const {
  static const auto description = std::string{"Path to the SSD storage. Can be a block device or a directory."};
  return description;
}

const std::string& BufferManagerPlugin::BufferManagerSSDPathSetting::get() {
  ssd_path = buffer_manager.config().ssd_path;
  return ssd_path;
}

void BufferManagerPlugin::BufferManagerSSDPathSetting::set(const std::string& value) {}

}  // namespace hyrise
