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
  static void _process_column_data_string(const std::shared_ptr<Table>& table, ColumnID column_id, std::vector<long> &process_column );
  static void _process_column_data_numeric(const std::shared_ptr<Table>& table, ColumnID column_id, std::vector<long> &process_column );
  static void _process_column_data_numeric_null(const std::shared_ptr<Table>& table, ColumnID column_id, std::vector<long> &process_column, std::vector<long> &null_column);
  // void _process_column_date(const std::shared_ptr<Table>& table, ColumnID column_id, std::vector<int> &process_column);
};

}  // namespace opossum
