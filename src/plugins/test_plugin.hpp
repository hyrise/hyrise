#pragma once

#include "storage/storage_manager.hpp"
#include "utils/abstract_plugin.hpp"

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
