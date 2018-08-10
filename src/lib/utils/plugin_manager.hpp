#pragma once

#include <unordered_map>

#include "types.hpp"
#include "utils/abstract_plugin.hpp"


namespace opossum {

struct PluginHandleWrapper {
  PluginHandle handle;
  AbstractPlugin* plugin;
};

using PluginName = std::string;

class PluginManager : private Noncopyable {
  friend class PluginManagerTest;

 public: 
  static PluginManager& get();

  void load_plugin(const std::string &path, const PluginName &name);

  void stop_plugin(const PluginName &name, bool should_erase = true);

  ~PluginManager();

  // deletes the entire PluginManager and creates a new one, used especially in tests
  // This can lead to a lot of issues if there are still running tasks / threads that
  // want to access a resource. You should be very sure that this is what you want.
  // Have a look at base_test.hpp to see the correct order of resetting things.
  static void reset();

 protected:
  PluginManager() {}
  PluginManager& operator=(PluginManager&&) = default;

  std::unordered_map<PluginName, PluginHandleWrapper> _plugins;

  bool _is_duplicate(AbstractPlugin* plugin);

  void _clean_up();
};
}  // namespace opossum
