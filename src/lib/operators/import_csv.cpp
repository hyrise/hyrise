#include "import_csv.hpp"

#include <memory>
#include <string>

#include "import_export/csv_non_rfc_parser.hpp"
#include "import_export/csv_rfc_parser.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

ImportCsv::ImportCsv(const std::string& filename, const optional<std::string> tablename, bool rfc_mode,
                     size_t buffer_size)
    : _filename(filename), _tablename(tablename), _rfc_mode(rfc_mode), _buffer_size(buffer_size) {}

const std::string ImportCsv::name() const { return "ImportCSV"; }

uint8_t ImportCsv::num_in_tables() const { return 0; }

uint8_t ImportCsv::num_out_tables() const { return 1; }

std::shared_ptr<const Table> ImportCsv::on_execute() {
  if (_tablename && StorageManager::get().has_table(*_tablename)) {
    return StorageManager::get().get_table(*_tablename);
  }

  // Check if file exists before giving it to the parser
  std::ifstream file(_filename);
  if (!file.is_open()) {
    throw std::runtime_error("ImportCsv: Could not find file " + _filename);
  }
  file.close();

  std::shared_ptr<Table> table;

  if (_rfc_mode) {
    CsvRfcParser parser{_buffer_size};
    table = parser.parse(_filename);
  } else {
    CsvNonRfcParser parser{_buffer_size};
    table = parser.parse(_filename);
  }

  if (_tablename) {
    StorageManager::get().add_table(*_tablename, table);
  }

  return table;
}

}  // namespace opossum
