#pragma once

#include "hyrise.hpp"
#include "utils/abstract_plugin.hpp"

namespace hyrise {

class TestPlugin : public AbstractPlugin {
 public:
  TestPlugin() : storage_manager(Hyrise::get().storage_manager) {}

  std::string description() const final;

  void start() final;

  void stop() final;

  std::vector<std::pair<PluginFunctionName, PluginFunctionPointer>> provided_user_executable_functions() final;

  void a_user_executable_function();

  static void a_static_user_executable_function();

  StorageManager& storage_manager;

 private:
  size_t _added_tables_count{0};
};

}  // namespace hyrise
