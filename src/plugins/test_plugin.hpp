#pragma once

#include "hyrise.hpp"
#include "utils/abstract_plugin.hpp"

namespace opossum {

class TestPlugin : public AbstractPlugin {
 public:
  TestPlugin() : storage_manager(Hyrise::get().storage_manager) {}

  std::string description() const final;

  void start() final;

  void stop() final;

  std::vector<std::pair<PluginFunctionName, PluginFunctionPointer>> provided_user_executable_functions() const final;

  void a_user_executable_function() const;

  void another_user_executable_function() const;

  StorageManager& storage_manager;
};

}  // namespace opossum
