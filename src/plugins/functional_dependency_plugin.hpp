#pragma once

#include <vector>

#include "gtest/gtest_prod.h"
#include "hyrise.hpp"
#include "types.hpp"
#include "utils/abstract_plugin.hpp"
#include "utils/singleton.hpp"

namespace opossum {

class FunctionalDependencyPlugin : public AbstractPlugin {
  friend class FunctionalDependencyPluginTest;

 public:
  FunctionalDependencyPlugin() : storage_manager(Hyrise::get().storage_manager) {}

  std::string description() const final;

  void start() final;

  void stop() final;

  StorageManager& storage_manager;

 private:
  static bool _check_dependency(const std::shared_ptr<Table>& table, std::vector<ColumnID> determinant,
                                std::vector<ColumnID> dependent);
};

}  // namespace opossum
