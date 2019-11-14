#include <dlfcn.h>

#include <filesystem>

#include "utils/abstract_plugin.hpp"
#include "utils/assert.hpp"

#include "plugin_manager.hpp"

namespace opossum {

bool PluginManager::_is_duplicate(const std::unique_ptr<AbstractPlugin>& plugin) const {
  for (const auto& [_, plugin_handle_wrapper] : _plugins) {
    if (typeid(plugin_handle_wrapper.plugin) == typeid(plugin)) {
      return true;
    }
  }

  return false;
}

void PluginManager::load_plugin(const std::filesystem::path& path) {
  const auto name = plugin_name_from_path(path);

  Assert(!_plugins.count(name), "Loading plugin failed: A plugin with name " + name + " already exists.");

  PluginHandle plugin_handle = dlopen(path.c_str(), static_cast<uint8_t>(RTLD_NOW) | static_cast<uint8_t>(RTLD_LOCAL));
  Assert(plugin_handle, std::string{"Loading plugin failed: "} + dlerror());

  // abstract_plugin.hpp defines a macro for exporting plugins which makes them instantiable by providing a
  // factory method. See the sources of AbstractPlugin and TestPlugin for further details.
  void* factory = dlsym(plugin_handle, "factory");
  Assert(factory,
         "Instantiating plugin failed: Use the EXPORT_PLUGIN (abstract_plugin.hpp) macro to export a factory method "
         "for your plugin!");

  using PluginGetter = AbstractPlugin* (*)();
  auto plugin_get = reinterpret_cast<PluginGetter>(factory);

  auto plugin = std::unique_ptr<AbstractPlugin>(plugin_get());
  PluginHandleWrapper plugin_handle_wrapper = {plugin_handle, std::move(plugin)};
  plugin = nullptr;

  Assert(!_is_duplicate(plugin_handle_wrapper.plugin),
         "Loading plugin failed: There can only be one instance of every plugin.");

  plugin_handle_wrapper.plugin->start();
  _plugins[name] = std::move(plugin_handle_wrapper);
}

void PluginManager::unload_plugin(const PluginName& name) {
  auto plugin_iter = _plugins.find(name);
  Assert(plugin_iter != _plugins.cend(), "Unloading plugin failed: A plugin with name " + name + " does not exist.");

  _unload_and_erase_plugin(plugin_iter);
}

std::unordered_map<PluginName, PluginHandleWrapper>::iterator PluginManager::_unload_and_erase_plugin(
    const std::unordered_map<PluginName, PluginHandleWrapper>::iterator plugin_iter) {
  const PluginName name = plugin_iter->first;
  const auto& plugin_handle_wrapper = plugin_iter->second;

  plugin_handle_wrapper.plugin->stop();
  const auto handle = plugin_handle_wrapper.handle;

  auto next = _plugins.erase(plugin_iter);

  dlclose(handle);

  return next;
}

void PluginManager::_clean_up() {
  for (auto plugin_iter = _plugins.begin(); plugin_iter != _plugins.end();) {
    plugin_iter = _unload_and_erase_plugin(plugin_iter);
  }
}

PluginManager::~PluginManager() { _clean_up(); }

}  // namespace opossum
