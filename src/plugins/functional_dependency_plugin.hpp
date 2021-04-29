#pragma once

#include <vector>


#include "hyrise.hpp"
#include "utils/abstract_plugin.hpp"
#include "utils/singleton.hpp"
#include "types.hpp"

namespace opossum {

class FunctionalDependencyPlugin : public AbstractPlugin {
  friend class FunctionalDependencyPluginTest;

 public:
  FunctionalDependencyPlugin() : storage_manager(Hyrise::get().storage_manager) {}

  std::string description() const final;

  void start() final;

  void stop() final;

  StorageManager& storage_manager;

  static bool _check_dependency(const std::shared_ptr<Table>& table, std::vector<ColumnID> determinant, std::vector<ColumnID> dependent);
};

}  // namespace opossum
