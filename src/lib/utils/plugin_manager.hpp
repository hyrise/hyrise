#pragma once

#include <unordered_map>

#include "types.hpp"
#include "utils/abstract_plugin.hpp"
#include "utils/filesystem.hpp"
#include "utils/singleton.hpp"
#include "utils/string_utils.hpp"

namespace opossum {

using PluginHandle = void*;
using PluginName = std::string;

struct PluginHandleWrapper {
  PluginHandle handle;
  AbstractPlugin* plugin;
};

class PluginManager : public Singleton<PluginManager> {
  friend class PluginManagerTest;
  friend class SingletonTest;

 public:
  void load_plugin(const filesystem::path& path);
  void unload_plugin(const PluginName& name);

  ~PluginManager();

  // Deletes the entire PluginManager and creates a new one, used especially in tests.
  // This can lead to a lot of issues if there are still running tasks / threads that
  // want to access a resource. You should be very sure that this is what you want.
  // Have a look at base_test.hpp to see the correct order of resetting things.
  static void reset();

 protected:
  friend class Singleton;

  PluginManager() {}
  const PluginManager& operator=(const PluginManager&) = delete;
  PluginManager& operator=(PluginManager&&) = default;

  std::unordered_map<PluginName, PluginHandleWrapper> _plugins;

  // This method is called during destruction and stops and unloads all currently loaded plugions.
  void _clean_up();
  bool _is_duplicate(AbstractPlugin* plugin) const;
  const std::unordered_map<PluginName, PluginHandleWrapper>::iterator _unload_erase_plugin(
      const std::unordered_map<PluginName, PluginHandleWrapper>::iterator it);
};
}  // namespace opossum
