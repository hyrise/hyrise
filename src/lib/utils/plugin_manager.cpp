#include <dlfcn.h>

#include "storage/storage_manager.hpp"
#include "utils/filesystem.hpp"
#include "utils/assert.hpp"

#include "plugin_manager.hpp"

namespace opossum {

// singleton
PluginManager& PluginManager::get() {
  static PluginManager instance;

  return instance;
}

bool PluginManager::_is_duplicate(AbstractPlugin* plugin) {
  for (auto &[plugin_name, plugin_handle_wrapper] : _plugins) {
    if (plugin_handle_wrapper.plugin == plugin) {
      return true;
    }
  }

  return false;
}


void PluginManager::load_plugin(const std::string &path, const PluginName &name) {
  DebugAssert(!_plugins.count(name), "Loading plugin failed: Name already in use.");

  PluginHandle plugin_handle = dlopen(path.c_str(), RTLD_NOW | RTLD_LOCAL);
  DebugAssert(plugin_handle, "Loading plugin failed: " + dlerror());

  void *factory = dlsym(plugin_handle , "factory");
  DebugAssert(factory, "Instantiating plugin failed: Have you implemented and exported the factory method?");

  typedef AbstractPlugin* (*Instantiator)(Injection);
  Instantiator instantiate = reinterpret_cast<Instantiator>(factory);


  Injection injection = {&(StorageManager::get())};


  auto plugin = instantiate(injection);
  PluginHandleWrapper plugin_handle_wrapper = {plugin_handle, plugin};
  DebugAssert(!_is_duplicate(plugin_handle_wrapper.plugin), "Loading plugin failed: There can only be one instance of every plugin.");

  _plugins[name] = plugin_handle_wrapper;

  plugin->start();
}


void PluginManager::stop_plugin(const PluginName &name) {
  auto plugin_handle_wrapper = _plugins.at(name);
  plugin_handle_wrapper.plugin->stop();
  dlclose(plugin_handle_wrapper.handle);

  _plugins.erase(name);
}


PluginManager::~PluginManager() {
  std::cout << "Calling ~PluginManager" << std::endl;
  for (auto &[plugin_name, plugin_handle_wrapper] : _plugins) {
    stop_plugin(plugin_name);
  }
};

}  // namespace opossum
