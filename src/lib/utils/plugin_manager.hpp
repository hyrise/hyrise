#pragma once

#include <filesystem>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "types.hpp"
#include "utils/abstract_plugin.hpp"
#include "utils/string_utils.hpp"

namespace hyrise {

using PluginHandle = void*;
using PluginName = std::string;

struct PluginNameFunctionNameHash {
  size_t operator()(const std::pair<PluginName, PluginFunctionName>& exec_function_identifier) const;
};

using UserExecutableFunctionMap =
    std::unordered_map<std::pair<PluginName, PluginFunctionName>, PluginFunctionPointer, PluginNameFunctionNameHash>;

struct PluginHandleWrapper {
  PluginHandle handle;
  std::unique_ptr<AbstractPlugin> plugin;
};

class PluginManager : public Noncopyable {
  friend class HyriseTest;
  friend class PluginManagerTest;
  friend class SingletonTest;

 public:
  void load_plugin(const std::filesystem::path& path);
  void unload_plugin(const PluginName& name);
  void exec_user_function(const PluginName& plugin_name, const PluginFunctionName& function_name);
  void exec_pre_benchmark_hook(const PluginName& plugin_name, AbstractBenchmarkItemRunner& benchmark_item_runner);
  void exec_post_benchmark_hook(const PluginName& plugin_name, nlohmann::json& report);

  std::vector<PluginName> loaded_plugins() const;

  UserExecutableFunctionMap user_executable_functions() const;

  bool has_pre_benchmark_hook(const PluginName& plugin_name) const;
  bool has_post_benchmark_hook(const PluginName& plugin_name) const;

  ~PluginManager();

 protected:
  PluginManager() = default;
  friend class Hyrise;

  const PluginManager& operator=(const PluginManager&) = delete;
  PluginManager& operator=(PluginManager&&) = default;

  std::unordered_map<PluginName, PluginHandleWrapper> _plugins;
  UserExecutableFunctionMap _user_executable_functions;
  std::unordered_map<PluginName, PreBenchmarkHook> _pre_benchmark_hooks;
  std::unordered_map<PluginName, PostBenchmarkHook> _post_benchmark_hooks;

  // This method is called during destruction and stops and unloads all currently loaded plugins.
  void _clean_up();
  bool _is_duplicate(const std::unique_ptr<AbstractPlugin>& plugin) const;
  std::unordered_map<PluginName, PluginHandleWrapper>::iterator _unload_and_erase_plugin(
      const std::unordered_map<PluginName, PluginHandleWrapper>::iterator plugin_iter);
};
}  // namespace hyrise
