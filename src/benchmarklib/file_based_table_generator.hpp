#pragma once

#include <filesystem>
#include <string>

#include "abstract_table_generator.hpp"

namespace opossum {

class FileBasedTableGenerator : public AbstractTableGenerator {
 public:
  FileBasedTableGenerator(const std::shared_ptr<BenchmarkConfig>& benchmark_config, const std::string& path);

 protected:
  std::unordered_map<std::string, TableEntry> _generate() override;

 private:
  const std::string _path;
};

}  // namespace opossum
