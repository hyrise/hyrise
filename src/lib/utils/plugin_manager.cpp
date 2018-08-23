#include <dlfcn.h>

#include "storage/storage_manager.hpp"
#include "utils/assert.hpp"
#include "utils/filesystem.hpp"

#include "plugin_manager.hpp"

namespace opossum {

bool PluginManager::_is_duplicate(AbstractPlugin* plugin) const {
  // This should work as soon as we support gcc-8 or gcc-8 supports us (https://gcc.gnu.org/bugzilla/show_bug.cgi?id=86740)
  // for ([[maybe_unused]] auto &[plugin_name, plugin_handle_wrapper] : _plugins) {
  //   if (plugin_handle_wrapper.plugin == plugin) {
  //     return true;
  //   }
  // }

  // return false;

  for (const auto& p : _plugins) {
    auto plugin_handle_wrapper = p.second;
    if (plugin_handle_wrapper.plugin == plugin) {
      return true;
    }
  }

  return false;
}

void PluginManager::load_plugin(const std::string& path, const PluginName& name) {
  Assert(!_plugins.count(name), "Loading plugin failed: A plugin with name  " + name + " already exists.");

  PluginHandle plugin_handle = dlopen(path.c_str(), RTLD_NOW | RTLD_LOCAL);
  Assert(plugin_handle, "Loading plugin failed: " + dlerror());

  void* factory = dlsym(plugin_handle, "factory");
  Assert(factory, "Instantiating plugin failed: Have you implemented and exported the factory method?");

  typedef AbstractPlugin* (*Instantiator)();
  Instantiator instantiate = reinterpret_cast<Instantiator>(factory);

  auto plugin = instantiate();
  PluginHandleWrapper plugin_handle_wrapper = {plugin_handle, plugin};
  Assert(!_is_duplicate(plugin_handle_wrapper.plugin),
         "Loading plugin failed: There can only be one instance of every plugin.");

  _plugins[name] = plugin_handle_wrapper;

  plugin->start();
  std::cout << "Plugin (" << name << ") successfully loaded." << std::endl;
}

void PluginManager::reset() { get() = PluginManager(); }

void PluginManager::stop_plugin(const PluginName& name) {
  auto plugin = _plugins.find(name);
  if (plugin != _plugins.cend()) {
    _stop_plugin(plugin);
  }
}

const std::unordered_map<PluginName, PluginHandleWrapper>::iterator PluginManager::_stop_plugin(
    const std::unordered_map<PluginName, PluginHandleWrapper>::iterator it) {
  const PluginName name = it->first;
  auto plugin_handle_wrapper = it->second;

  plugin_handle_wrapper.plugin->stop();
  dlclose(plugin_handle_wrapper.handle);

  auto next = _plugins.erase(it);

  std::cout << "Plugin (" << name << ") stopped." << std::endl;
  return next;
}

void PluginManager::_clean_up() {
  for (auto it = _plugins.begin(); it != _plugins.end();) {
    it = _stop_plugin(it);
  }
}

PluginManager::~PluginManager() { _clean_up(); }

}  // namespace opossum
