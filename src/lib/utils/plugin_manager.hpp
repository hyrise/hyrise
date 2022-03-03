#pragma once

#include <filesystem>
#include <unordered_map>

#include "types.hpp"
#include "utils/abstract_plugin.hpp"
#include "utils/string_utils.hpp"

namespace opossum {

using PluginHandle = void*;
using PluginName = std::string;
using Keyword = std::string;

struct PluginHandleWrapper {
  PluginHandle handle;
  std::unique_ptr<AbstractPlugin> plugin;
};

struct string_pair_hash {
  size_t operator () (const std::pair<PluginName, Keyword> &p) const {
    size_t hash{0};
    boost::hash_combine(hash, p.first);
    boost::hash_combine(hash, p.second);

    return hash;
  }
};

class PluginManager : public Noncopyable {
  friend class HyriseTest;
  friend class PluginManagerTest;
  friend class SingletonTest;

 public:
  void load_plugin(const std::filesystem::path& path);
  void unload_plugin(const PluginName& name);
  void exec_function(const PluginName& plugin_name, const std::string& keyword);

  std::vector<PluginName> loaded_plugins() const;

  ~PluginManager();

 protected:
  PluginManager() = default;
  friend class Hyrise;

  const PluginManager& operator=(const PluginManager&) = delete;
  PluginManager& operator=(PluginManager&&) = default;

  std::unordered_map<PluginName, PluginHandleWrapper> _plugins;
  std::unordered_map<std::pair<PluginName, Keyword>, std::function<void(void)>, string_pair_hash> _keyword_function_map;

  // This method is called during destruction and stops and unloads all currently loaded plugions.
  void _clean_up();
  bool _is_duplicate(const std::unique_ptr<AbstractPlugin>& plugin) const;
  std::unordered_map<PluginName, PluginHandleWrapper>::iterator _unload_and_erase_plugin(
      const std::unordered_map<PluginName, PluginHandleWrapper>::iterator plugin_iter);
};
}  // namespace opossum
