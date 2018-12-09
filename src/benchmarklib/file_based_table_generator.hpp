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
    std::optional<std::filesystem::path> binary_file_path;
    std::optional<std::filesystem::path> text_file_path;
    bool loaded_from_binary{false};
    bool reencoded{false};
    std::shared_ptr<Table> table;
  };

  std::unordered_map<std::string, TableEntry> _table_entries;

  const std::string _path;
};

}  // namespace opossum
