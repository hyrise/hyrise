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

  void stop_plugin(const PluginName &name);

  ~PluginManager();

 private:
  std::unordered_map<PluginName, PluginHandleWrapper> _plugins;

  bool _is_duplicate(AbstractPlugin* plugin);
};
}  // namespace opossum
