#include "import_csv.hpp"

#include <fstream>
#include <memory>
#include <string>

#include "import_export/csv_parser.hpp"
#include "storage/storage_manager.hpp"

#include "utils/assert.hpp"

namespace opossum {

ImportCsv::ImportCsv(const std::string& filename, const optional<std::string> tablename)
    : _filename(filename), _tablename(tablename), _config(CsvConfig{}) {}

ImportCsv::ImportCsv(const std::string& filename, const CsvConfig& config, const optional<std::string> tablename)
    : _filename(filename), _tablename(tablename), _config(config) {}

const std::string ImportCsv::name() const { return "ImportCSV"; }

uint8_t ImportCsv::num_in_tables() const { return 0; }

uint8_t ImportCsv::num_out_tables() const { return 1; }

std::shared_ptr<const Table> ImportCsv::_on_execute() {
  if (_tablename && StorageManager::get().has_table(*_tablename)) {
    return StorageManager::get().get_table(*_tablename);
  }

  // Check if file exists before giving it to the parser
  std::ifstream file(_filename);
  Assert(file.is_open(), "ImportCsv: Could not find file " + _filename);
  file.close();

  std::shared_ptr<Table> table;
  CsvParser parser{_config};
  table = parser.parse(_filename);

  if (_tablename) {
    StorageManager::get().add_table(*_tablename, table);
  }

  return table;
}

}  // namespace opossum
