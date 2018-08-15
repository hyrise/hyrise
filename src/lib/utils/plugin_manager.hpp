#pragma once

#include <unordered_map>

#include "types.hpp"
#include "utils/abstract_plugin.hpp"
#include "utils/singleton.hpp"

namespace opossum {

struct PluginHandleWrapper {
  PluginHandle handle;
  AbstractPlugin* plugin;
};

class PluginManager : public Singleton<PluginManager> {
  friend class PluginManagerTest;
  friend class SingletonTest;

 public:
  // Singleton
  inline static PluginManager& get() {
    static PluginManager instance;

    return instance;
  }

  void load_plugin(const std::string& path, const PluginName& name);
  void stop_plugin(const PluginName& name);

  ~PluginManager();

  // Deletes the entire PluginManager and creates a new one, used especially in tests.
  // This can lead to a lot of issues if there are still running tasks / threads that
  // want to access a resource. You should be very sure that this is what you want.
  // Have a look at base_test.hpp to see the correct order of resetting things.
  static void reset();

  PluginManager(PluginManager&&) = delete;

 protected:
  friend class Singleton;

  PluginManager() {}
  const PluginManager& operator=(const PluginManager&) = delete;
  PluginManager& operator=(PluginManager&&) = default;

  std::unordered_map<PluginName, PluginHandleWrapper> _plugins;

  bool _is_duplicate(AbstractPlugin* plugin) const;
  void _clean_up();
};
}  // namespace opossum
