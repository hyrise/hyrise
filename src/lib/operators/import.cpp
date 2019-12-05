#include "import.hpp"

#include <boost/algorithm/string.hpp>

#include "hyrise.hpp"
#include "import_export/binary/binary_parser.hpp"
#include "import_export/csv/csv_parser.hpp"
#include "utils/assert.hpp"
#include "utils/load_table.hpp"

namespace opossum {

Import::Import(const std::string& filename, const std::optional<std::string>& tablename, const ChunkOffset chunk_size,
               const FileType type, const std::optional<CsvMeta>& csv_meta)
    : AbstractReadOnlyOperator(OperatorType::Import),
      _filename(filename),
      _tablename(tablename),
      _chunk_size(chunk_size),
      _type(type),
      _csv_meta(csv_meta) {}

const std::string& Import::name() const {
  static const auto name = std::string{"Import"};
  return name;
}

std::shared_ptr<const Table> Import::_on_execute() {
  if (_tablename && Hyrise::get().storage_manager.has_table(*_tablename)) {
    return Hyrise::get().storage_manager.get_table(*_tablename);
  }

  // Check if file exists before giving it to the parser
  std::ifstream file(_filename);
  Assert(file.is_open(), "Import: Could not find file " + _filename);
  file.close();

  const auto table = _import();

  if (_tablename) {
    Hyrise::get().storage_manager.add_table(*_tablename, table);
  }

  return table;
}

std::shared_ptr<AbstractOperator> Import::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<Import>(_filename, _tablename, _chunk_size, _type, _csv_meta);
}

void Import::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

std::shared_ptr<Table> Import::_import() {
  switch (_type) {
    case FileType::Csv:
      return CsvParser::parse(_filename, _chunk_size, _csv_meta);
    case FileType::Tbl:
      return load_table(_filename, _chunk_size);
    case FileType::Binary:
      return BinaryParser::parse(_filename);
    case FileType::Auto:
      return _import_any_type();
    default:
      Fail("Import: Cannot import file type.");
  }
}

std::shared_ptr<Table> Import::_import_any_type() {
  const auto extension = std::string{std::filesystem::path{_filename}.extension()};
  if (extension == ".csv") {
    return CsvParser::parse(_filename, _chunk_size, _csv_meta);
  } else if (extension == ".tbl") {
    return load_table(_filename, _chunk_size);
  } else if (extension == ".bin") {
    return BinaryParser::parse(_filename);
  }

  Fail("Import: Cannot import file type.");
}

}  // namespace opossum
