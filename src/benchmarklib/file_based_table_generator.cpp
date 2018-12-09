#include "file_based_table_generator.hpp"

#include <boost/algorithm/string.hpp>

#include "benchmark_config.hpp"
#include "benchmark_table_encoder.hpp"
#include "operators/import_binary.hpp"
#include "import_export/csv_parser.hpp"
#include "utils/load_table.hpp"

using namespace std::string_literals;

namespace opossum {

FileBasedTableGenerator::FileBasedTableGenerator(const std::shared_ptr<BenchmarkConfig>& benchmark_config, const std::string &path) :
  AbstractTableGenerator(benchmark_config), _path(path)
{

}

std::unordered_map<std::string, AbstractTableGenerator::TableEntry> FileBasedTableGenerator::_generate() {
  // TODO(moritz)
  Assert(!std::filesystem::is_regular_file(_path), "");

  auto table_entries = std::unordered_map<std::string, TableEntry>{};
  const auto table_extensions = std::unordered_set<std::string>{".csv", ".tbl", ".bin"};

  /**
   * 1. Explore the directory and identify tables to be loaded
   * Recursively walk through the specified directory and collect all tables found on the way. A tables name is
   * determined by its filename. Multiple file extensions per table are allowed, for example there could be a CSV and a
   * binary version of a table.
   */
  for (const auto &directory_entry : filesystem::recursive_directory_iterator(_path)) {
    if (!std::filesystem::is_regular_file(directory_entry)) continue;

    const auto extension = directory_entry.path().extension();

    if (table_extensions.find(extension) == table_extensions.end()) continue;

    auto table_name = directory_entry.path().filename();
    table_name.replace_extension("");

    auto table_entries_iter = table_entries.find(table_name);

    if (table_entries_iter == table_entries.end()) {
      table_entries_iter = table_entries.emplace(table_name, TableEntry{}).first;
    }

    auto& table_entry = table_entries_iter->second;

    if (extension == ".bin") {
      Assert(!table_entry.binary_file_path, "Multiple binary files found for table '"s + table_name.string() + "'");
      table_entry.binary_file_path = directory_entry.path();
    } else {
      Assert(!table_entry.text_file_path, "Multiple text files found for table '"s + table_name.string() + "'");
      table_entry.text_file_path = directory_entry.path();
    }
  }

  /**
   * 2. Actually load the tables. Load from binary file if a binary file exists for a Table.
   */
  for (auto& [table_name, table_entry] : table_entries) {
    _benchmark_config->out << "- Loading table '" << table_name << "' ";

    // Pick a source file to load a table from, prefer the binary version
    if (table_entry.binary_file_path) {
      _benchmark_config->out << " from '" << *table_entry.binary_file_path << std::endl;
      table_entry.table = ImportBinary::read_binary(*table_entry.binary_file_path);
      table_entry.loaded_from_binary = true;
    } else {
      _benchmark_config->out << " from '" << *table_entry.text_file_path << std::endl;
      const auto extension = table_entry.text_file_path->extension();
      if (extension == ".tbl") {
        table_entry.table = load_table(*table_entry.text_file_path, _benchmark_config->chunk_size);
      } else if (extension == ".csv") {
        table_entry.table = CsvParser{}.parse(*table_entry.text_file_path, std::nullopt, _benchmark_config->chunk_size);
      } else {
        Fail("Unknown textual file format. This should have been caught earlier.");
      }

    }
  }

  return table_entries;
}
}

