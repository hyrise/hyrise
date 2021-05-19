#pragma once

#include <filesystem>
#include <string>

#include "abstract_table_generator.hpp"

namespace opossum {

class FileBasedTableGenerator : virtual public AbstractTableGenerator {
 public:
  FileBasedTableGenerator(const std::shared_ptr<BenchmarkConfig>& benchmark_config, const std::string& path);

  std::unordered_map<std::string, BenchmarkTableInfo> generate() override;

  /**
   * @param add_constraints_callback is called after the table data has been added to the StorageManager.
   * It should be used to define table constraints, if available.
   */
  void set_add_constraints_callback(const std::function<void()>& add_constraints_callback);

 protected:
  void _add_constraints(std::unordered_map<std::string, BenchmarkTableInfo>& table_info_by_name) const override;
  const std::string _path;
  std::function<void()> _add_constraints_callback;
};

}  // namespace opossum
