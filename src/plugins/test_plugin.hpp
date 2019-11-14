#pragma once

#include "hyrise.hpp"
#include "utils/abstract_plugin.hpp"
#include "utils/singleton.hpp"

namespace opossum {

class TestPlugin : public AbstractPlugin {
 public:
  TestPlugin() : storage_manager(Hyrise::get().storage_manager) {}

  const std::string description() const final;

  void start() final;

  void stop() final;

  StorageManager& storage_manager;
};

}  // namespace opossum
