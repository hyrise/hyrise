#pragma once

#include "hyrise.hpp"
#include "utils/abstract_plugin.hpp"
#include "utils/singleton.hpp"

namespace opossum {

class TestPlugin : public AbstractPlugin {
 public:
  TestPlugin() : storage_manager(Hyrise::get().storage_manager) {}

  std::string description() const final;

  void start() final;

  void stop() final;

  std::vector<std::pair<std::string, std::function<void(void)>>> keywords_functions() const final;

  void test_user_callable_function() const;

  StorageManager& storage_manager;
};

}  // namespace opossum
