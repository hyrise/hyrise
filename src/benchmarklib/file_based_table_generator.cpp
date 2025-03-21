#include "file_based_table_generator.hpp"

#include <filesystem>
#include <functional>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "abstract_table_generator.hpp"
#include "benchmark_config.hpp"
#include "hyrise.hpp"
#include "import_export/binary/binary_parser.hpp"
#include "import_export/csv/csv_parser.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/job_task.hpp"
#include "utils/assert.hpp"
#include "utils/list_directory.hpp"
#include "utils/load_table.hpp"
#include "utils/timer.hpp"

namespace hyrise {

FileBasedTableGenerator::FileBasedTableGenerator(const std::shared_ptr<BenchmarkConfig>& benchmark_config,
                                                 const std::string& path)
    : AbstractTableGenerator(benchmark_config), _path(path) {}

std::unordered_map<std::string, BenchmarkTableInfo> FileBasedTableGenerator::generate() {
  Assert(std::filesystem::is_directory(_path), std::string{"Table path "} + _path + " must be a directory.");

  auto table_info_by_name = std::unordered_map<std::string, BenchmarkTableInfo>{};
  const auto table_extensions = std::unordered_set<std::string>{".csv", ".tbl", ".bin"};

  /**
   * 1. Explore the directory and identify tables to be loaded
   * Recursively walk through the specified directory and collect all tables found on the way. A tables name is
   * determined by its filename. Multiple file extensions per table are allowed, for example there could be a CSV and a
   * binary version of a table.
   */
  for (const auto& directory_entry : list_directory(_path)) {
    const auto extension = directory_entry.extension();

    if (table_extensions.find(extension) == table_extensions.end()) {
      continue;
    }

    auto table_name = directory_entry.filename();
    table_name.replace_extension("");

    auto table_info_by_name_iter = table_info_by_name.find(table_name);

    if (table_info_by_name_iter == table_info_by_name.end()) {
      table_info_by_name_iter = table_info_by_name.emplace(table_name, BenchmarkTableInfo{}).first;
    }

    auto& table_info = table_info_by_name_iter->second;

    if (extension == ".bin") {
      Assert(!table_info.binary_file_path,
             std::string{"Multiple binary files found for table '"} + table_name.string() + "'");
      table_info.binary_file_path = directory_entry;
    } else {
      Assert(!table_info.text_file_path,
             std::string{"Multiple text files found for table '"} + table_name.string() + "'");
      table_info.text_file_path = directory_entry;
    }
  }

  /**
   * 2. Check for "out of date" binary files, i.e., whether both a binary and textual file exists AND the
   *    binary file is older than the textual file.
   */
  for (auto& [table_name, table_info] : table_info_by_name) {
    if (table_info.binary_file_path && table_info.text_file_path) {
      const auto last_binary_write = std::filesystem::last_write_time(*table_info.binary_file_path);
      const auto last_text_write = std::filesystem::last_write_time(*table_info.text_file_path);

      if (last_binary_write < last_text_write) {
        std::cout << "-  Binary file '" << (*table_info.binary_file_path)
                  << "' is out of date and needs to be re-exported\n";
        table_info.binary_file_out_of_date = true;
      }
    }
  }

  /**
   * 3. Actually load the tables. Load from binary file if a up-to-date binary file exists for a table.
   */
  auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  jobs.reserve(table_info_by_name.size());
  for (auto& table_name_info_pair : table_info_by_name) {
    jobs.emplace_back(std::make_shared<JobTask>([&]() {
      auto timer = Timer{};
      auto message = std::stringstream{};
      message << "-  Loaded table '" << table_name_info_pair.first << "' ";
      auto& table_info = table_name_info_pair.second;

      // Pick a source file to load a table from, prefer the binary version.
      if (table_info.binary_file_path && !table_info.binary_file_out_of_date) {
        message << "from " << *table_info.binary_file_path;
        table_info.table = BinaryParser::parse(*table_info.binary_file_path);
        table_info.loaded_from_binary = true;
      } else {
        message << "from " << *table_info.text_file_path;
        const auto extension = table_info.text_file_path->extension();
        if (extension == ".tbl") {
          table_info.table = load_table(*table_info.text_file_path, _benchmark_config->chunk_size);
        } else if (extension == ".csv") {
          table_info.table = CsvParser::parse(*table_info.text_file_path, _benchmark_config->chunk_size);
        } else {
          Fail("Unknown textual file format. This should have been caught earlier.");
        }
      }

      message << " (" << table_info.table->row_count() << " rows; " << timer.lap_formatted() << ")\n";
      std::cout << message.str() << std::flush;
    }));
  }

  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);
  return table_info_by_name;
}

void FileBasedTableGenerator::set_add_constraints_callback(
    const std::function<void(std::unordered_map<std::string, BenchmarkTableInfo>&)>& add_constraints_callback) {
  _add_constraints_callback = add_constraints_callback;
}

void FileBasedTableGenerator::_add_constraints(
    std::unordered_map<std::string, BenchmarkTableInfo>& table_info_by_name) const {
  if (_add_constraints_callback) {
    _add_constraints_callback(table_info_by_name);
  }
}

}  // namespace hyrise
