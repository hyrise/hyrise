#pragma once

#include <filesystem>
#include <string>

#include "abstract_table_generator.hpp"

namespace opossum {

class FileBasedTableGenerator : public AbstractTableGenerator {
 public:
  FileBasedTableGenerator(const std::shared_ptr<BenchmarkConfig>& benchmark_config, const std::string& path);

 protected:
  void _generate() override;

 private:
  struct TableEntry {
    std::unordered_map<std::string, std::filesystem::path> source_files_by_extension;
  };

  std::unordered_map<std::string, TableEntry> _table_entries;

  const std::string _path;
};

}  // namespace opossum
