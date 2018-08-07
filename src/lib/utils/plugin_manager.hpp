#pragma once

#include <unordered_map>

#include "types.hpp"
#include "/Users/jan/Desktop/hyrise-plugins/src/AbstractPlugin.hpp"


namespace opossum {

// class AbstractPlugin;

// TODO: Find name
struct PluginHandleWrapper {
  PluginHandle handle;
  AbstractPlugin* plugin;
};

using PluginName = std::string;

class PluginManager : private Noncopyable {
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
