#include "plugin_manager.hpp"

#include <dlfcn.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/container_hash/hash.hpp>

#include "nlohmann/json.hpp"

#include "abstract_benchmark_item_runner.hpp"
#include "hyrise.hpp"
#include "utils/abstract_plugin.hpp"
#include "utils/assert.hpp"
#include "utils/log_manager.hpp"
#include "utils/string_utils.hpp"

namespace hyrise {

bool PluginManager::_is_duplicate(const std::unique_ptr<AbstractPlugin>& plugin) const {
  const auto& plugin_ref = *plugin;
  for (const auto& [_, plugin_handle_wrapper] : _plugins) {
    const auto& existing_plugin_ref = *plugin_handle_wrapper.plugin;
    if (typeid(existing_plugin_ref) == typeid(plugin_ref)) {
      return true;
    }
  }

  return false;
}

std::vector<PluginName> PluginManager::loaded_plugins() const {
  auto plugin_names = std::vector<std::string>{};
  plugin_names.reserve(_plugins.size());

  for (const auto& [plugin_name, _] : _plugins) {
    plugin_names.emplace_back(plugin_name);
  }

  std::sort(plugin_names.begin(), plugin_names.end());

  return plugin_names;
}

std::unordered_map<std::pair<PluginName, PluginFunctionName>, PluginFunctionPointer, PluginNameFunctionNameHash>
PluginManager::user_executable_functions() const {
  return _user_executable_functions;
}

void PluginManager::load_plugin(const std::filesystem::path& path) {
  const auto plugin_name = plugin_name_from_path(path);

  Assert(!_plugins.contains(plugin_name),
         "Loading plugin failed: A plugin with name " + plugin_name + " already exists.");

  // Lock for dl* functions (see clang-tidy's "concurrency-mt-unsafe")
  static auto dl_mutex = std::mutex{};
  auto lock = std::lock_guard<std::mutex>{dl_mutex};

  PluginHandle plugin_handle = dlopen(path.c_str(), static_cast<uint8_t>(RTLD_NOW) | static_cast<uint8_t>(RTLD_LOCAL));
  // NOLINTNEXTLINE(concurrency-mt-unsafe): dlerror is not thread-safe, but it is guarded by dl_mutex.
  Assert(plugin_handle, std::string{"Loading plugin failed: "} + dlerror());

  // abstract_plugin.hpp defines a macro for exporting plugins which makes them instantiable by providing a
  // factory method. See the sources of AbstractPlugin and TestPlugin for further details.
  void* factory = dlsym(plugin_handle, "factory");
  Assert(factory,
         "Instantiating plugin failed: Use the EXPORT_PLUGIN (see abstract_plugin.hpp) macro to export a factory "
         "method for your plugin.");

  using PluginGetter = AbstractPlugin* (*)();
  auto plugin_get = reinterpret_cast<PluginGetter>(factory);

  auto plugin = std::unique_ptr<AbstractPlugin>(plugin_get());
  PluginHandleWrapper plugin_handle_wrapper = {plugin_handle, std::move(plugin)};
  plugin = nullptr;

  Assert(!_is_duplicate(plugin_handle_wrapper.plugin),
         "Loading plugin failed: There can only be one instance of every plugin.");

  plugin_handle_wrapper.plugin->start();
  _plugins[plugin_name] = std::move(plugin_handle_wrapper);

  // Add the newly loaded plugin's user executable function and benchmark hooks to our maps of functions.
  auto& plugin_ref = *_plugins[plugin_name].plugin;
  const auto& user_executable_functions = plugin_ref.provided_user_executable_functions();
  for (const auto& [function_name, function_pointer] : user_executable_functions) {
    _user_executable_functions[{plugin_name, function_name}] = function_pointer;
  }
  const auto& pre_benchmark_hook = plugin_ref.pre_benchmark_hook();
  if (pre_benchmark_hook) {
    _pre_benchmark_hooks[plugin_name] = *pre_benchmark_hook;
  }
  const auto& post_benchmark_hook = plugin_ref.post_benchmark_hook();
  if (post_benchmark_hook) {
    _post_benchmark_hooks[plugin_name] = *post_benchmark_hook;
  }
}

void PluginManager::exec_user_function(const PluginName& plugin_name, const PluginFunctionName& function_name) {
  Assert(_user_executable_functions.contains({plugin_name, function_name}),
         "There is no function '" + function_name + "' defined for plugin '" + plugin_name + "'.");

  const auto& user_executable_function = _user_executable_functions[{plugin_name, function_name}];
  user_executable_function();

  auto& log_manager = Hyrise::get().log_manager;
  log_manager.add_message(
      "PluginManager",
      "Called user executable function '" + function_name + "' provided by plugin '" + plugin_name + "'.",
      LogLevel::Info);
}

void PluginManager::exec_pre_benchmark_hook(const PluginName& plugin_name,
                                            AbstractBenchmarkItemRunner& benchmark_item_runner) {
  Assert(_pre_benchmark_hooks.contains({plugin_name}),
         "There is no pre-benchmark hook defined for plugin '" + plugin_name + "'.");

  const auto& pre_benchmark_hook = _pre_benchmark_hooks[plugin_name];
  pre_benchmark_hook(benchmark_item_runner);

  auto& log_manager = Hyrise::get().log_manager;
  log_manager.add_message("PluginManager", "Called pre-benchmark hook provided by plugin '" + plugin_name + "'.",
                          LogLevel::Info);
}

void PluginManager::exec_post_benchmark_hook(const PluginName& plugin_name, nlohmann::json& report) {
  Assert(_post_benchmark_hooks.contains({plugin_name}),
         "There is no post-benchmark hook defined for plugin '" + plugin_name + "'.");

  const auto& post_benchmark_hook = _post_benchmark_hooks[plugin_name];
  post_benchmark_hook(report);

  auto& log_manager = Hyrise::get().log_manager;
  log_manager.add_message("PluginManager", "Called post-benchmark hook provided by plugin '" + plugin_name + "'.",
                          LogLevel::Info);
}

bool PluginManager::has_pre_benchmark_hook(const PluginName& plugin_name) const {
  return _pre_benchmark_hooks.contains(plugin_name);
}

bool PluginManager::has_post_benchmark_hook(const PluginName& plugin_name) const {
  return _post_benchmark_hooks.contains(plugin_name);
}

void PluginManager::unload_plugin(const PluginName& plugin_name) {
  auto plugin_iter = _plugins.find(plugin_name);
  Assert(plugin_iter != _plugins.cend(),
         "Unloading plugin failed: A plugin with name '" + plugin_name + "' does not exist.");

  _unload_and_erase_plugin(plugin_iter);
}

std::unordered_map<PluginName, PluginHandleWrapper>::iterator PluginManager::_unload_and_erase_plugin(
    const std::unordered_map<PluginName, PluginHandleWrapper>::iterator plugin_iter) {
  const PluginName plugin_name = plugin_iter->first;
  const auto& plugin_handle_wrapper = plugin_iter->second;

  // Delete user exectuable functions and benchmark hooks of the plugin to be unloaded from the maps of functions.
  std::erase_if(_user_executable_functions, [&](const auto& item) {
    const auto& [item_plugin_name, _] = item.first;
    return item_plugin_name == plugin_name;
  });
  _pre_benchmark_hooks.erase(plugin_name);
  _post_benchmark_hooks.erase(plugin_name);

  plugin_handle_wrapper.plugin->stop();
  auto* const handle = plugin_handle_wrapper.handle;

  auto next = _plugins.erase(plugin_iter);

  dlclose(handle);

  return next;
}

void PluginManager::_clean_up() {
  for (auto plugin_iter = _plugins.begin(); plugin_iter != _plugins.end();) {
    plugin_iter = _unload_and_erase_plugin(plugin_iter);
  }
}

PluginManager::~PluginManager() {
  _clean_up();
}

size_t PluginNameFunctionNameHash::operator()(
    const std::pair<PluginName, PluginFunctionName>& exec_function_identifier) const {
  auto hash = size_t{0};
  boost::hash_combine(hash, exec_function_identifier.first);
  boost::hash_combine(hash, exec_function_identifier.second);
  return hash;
}

}  // namespace hyrise
