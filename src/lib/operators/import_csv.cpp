#include "import_csv.hpp"

#include <fstream>
#include <memory>
#include <optional>
#include <string>

#include "import_export/csv_parser.hpp"
#include "storage/storage_manager.hpp"
#include "utils/assert.hpp"

namespace opossum {

ImportCsv::ImportCsv(const std::string& filename, const std::optional<std::string>& tablename,
                     const std::optional<CsvMeta>& csv_meta)
    : AbstractReadOnlyOperator(OperatorType::ImportCsv),
      _filename(filename),
      _tablename(tablename),
      _csv_meta(csv_meta) {}

ImportCsv::ImportCsv(const std::string& filename, const std::optional<CsvMeta>& csv_meta,
                     const std::optional<std::string>& tablename)
    : ImportCsv(filename, tablename, csv_meta) {}

const std::string ImportCsv::name() const { return "ImportCSV"; }

std::shared_ptr<const Table> ImportCsv::_on_execute() {
  if (_tablename && StorageManager::get().has_table(*_tablename)) {
    return StorageManager::get().get_table(*_tablename);
  }

  // Check if file exists before giving it to the parser
  std::ifstream file(_filename);
  Assert(file.is_open(), "ImportCsv: Could not find file " + _filename);
  file.close();

  std::shared_ptr<Table> table;
  CsvParser parser;
  table = parser.parse(_filename, _csv_meta);

  if (_tablename) {
    StorageManager::get().add_table(*_tablename, table);
  }

  return table;
}

std::shared_ptr<AbstractOperator> ImportCsv::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<ImportCsv>(_filename, _tablename, _csv_meta);
}

void ImportCsv::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

}  // namespace opossum
