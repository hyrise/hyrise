#pragma once

#include "storage/storage_manager.hpp" // NEEDEDINCLUDE
#include "utils/abstract_plugin.hpp" // NEEDEDINCLUDE

namespace opossum {

class TestPlugin : public AbstractPlugin, public Singleton<TestPlugin> {
 public:
  TestPlugin() : sm(StorageManager::get()) {}

  const std::string description() const final;

  void start() final;

  void stop() final;

  StorageManager& sm;
};

}  // namespace opossum
