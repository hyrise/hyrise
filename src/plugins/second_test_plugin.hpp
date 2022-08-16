#pragma once

#include "hyrise.hpp"
#include "utils/abstract_plugin.hpp"

namespace hyrise {

class SecondTestPlugin : public AbstractPlugin {
 public:
  SecondTestPlugin() : storage_manager(Hyrise::get().storage_manager) {}

  std::string description() const final;

  void start() final;

  void stop() final;

  std::vector<std::pair<PluginFunctionName, PluginFunctionPointer>> provided_user_executable_functions() final;

  void a_user_executable_function() const;

  StorageManager& storage_manager;
};

}  // namespace hyrise
