#include "file_based_table_generator.hpp"

#include <boost/algorithm/string.hpp>

#include "benchmark_config.hpp"
#include "import_export/binary.hpp"
#include "operators/import_binary.hpp"
#include "import_export/csv_parser.hpp"
#include "utils/load_table.hpp"

using namespace std::string_literals;

namespace opossum {

//void _load_table_folder(const BenchmarkConfig &benchmark_config, const std::string &table_path) {
//
//  filesystem::path path{table_path};
//  Assert(filesystem::exists(path), "No such file or directory '" + table_path + "'");
//
//  std::vector<std::string> tables;
//
//  // If only one file was specified, add it and return
//  if (filesystem::is_regular_file(path)) {
//    Assert(is_table_file(table_path), "Specified file '" + table_path + "' is not a .csv or .tbl file");
//    tables.push_back(table_path);
//  } else {
//    // Recursively walk through the specified directory and add all files on the way
//    for (const auto &entry : filesystem::recursive_directory_iterator(path)) {
//      const auto filename = entry.path().string();
//      if (filesystem::is_regular_file(entry) && is_table_file(filename)) {
//        tables.push_back(filename);
//      }
//    }
//  }
//
//  Assert(!tables.empty(), "No tables found in '" + table_path + "'");
//
//  for (const auto &table_path_str : tables) {
//    const auto table_name = filesystem::path{table_path_str}.stem().string();
//
//    std::shared_ptr<Table> table;
//    if (boost::algorithm::ends_with(table_path_str, ".tbl")) {
//      table = load_table(table_path_str, benchmark_config.chunk_size);
//    } else {
//      table = CsvParser{}.parse(table_path_str, std::nullopt, benchmark_config.chunk_size);
//    }
//
//    benchmark_config.out << "- Adding table '" << table_name << "'" << std::endl;
//    BenchmarkTableEncoder::encode(table_name, table, benchmark_config.encoding_config, benchmark_config.out);
//    StorageManager::get().add_table(table_name, table);
//  }
//}

FileBasedTableGenerator::FileBasedTableGenerator(const std::shared_ptr<BenchmarkConfig>& benchmark_config, const std::string &path) :
AbstractTableGenerator(benchmark_config), _path(path)
{

}

void FileBasedTableGenerator::_generate() {
  // TODO(moritz)
  Assert(!std::filesystem::is_regular_file(_path), "")

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

    auto table_entries_iter = _table_entries.find(table_name);

    if (table_entries_iter == _table_entries.end()) {
      table_entries_iter = _table_entries.emplace(table_name, TableEntry{}).first;
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
  for (auto& [table_name, table_entry] : _table_entries) {
    _benchmark_config->out << "- Loading table '" << table_name << "'" << std::endl;

    // Pick a source file to load a table from, prefer the binary version
    if (table_entry.binary_file_path) {
      auto import_operator = ImportBinary{*table_entry.binary_file_path};
      import_operator.execute();
      table_entry.table = import_operator.get_output();
      table_entry.loaded_from_binary = true;
    } else {
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

  /**
   * 3. Encode the Tables
   */
  for (auto& [table_name, table_entry] : _table_entries) {

  }

  exit(0);
}
}

using namespace std::string_literals;  // NOLINE
