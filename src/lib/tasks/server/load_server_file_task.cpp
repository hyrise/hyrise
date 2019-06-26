#include "load_server_file_task.hpp"
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <operators/import_binary.hpp>
#include <operators/import_csv.hpp>

#include "hyrise.hpp"
#include "storage/chunk.hpp"
#include "utils/load_table.hpp"

namespace opossum {

void LoadServerFileTask::_on_execute() {
  try {
    std::vector<std::string> file_parts;
    boost::algorithm::split(file_parts, _file_name, boost::is_any_of("."));
    const std::string& extension = file_parts.back();

    if (extension == "csv") {
      auto importer = std::make_shared<ImportCsv>(_file_name, Chunk::MAX_SIZE, _table_name);
      importer->execute();
    } else if (extension == "tbl") {
      const auto table = load_table(_file_name, Chunk::MAX_SIZE);
      Hyrise::get().storage_manager.add_table(_table_name, table);
    } else if (extension == "bin") {
      auto importer = std::make_shared<ImportBinary>(_file_name, _table_name);
      importer->execute();
    } else {
      Fail("Unsupported file type could not be loaded. Only csv, tbl, and bin are supported.");
    }

    _promise.set_value();
  } catch (...) {
    _promise.set_exception(boost::current_exception());
  }
}

}  // namespace opossum
