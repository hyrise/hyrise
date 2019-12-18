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
  // Check if file exists before giving it to the parser
  std::ifstream file(_filename);
  Assert(file.is_open(), "Import: Could not find file " + _filename);
  file.close();

  std::shared_ptr<Table> table;

  if (_type == FileType::Auto) {
    _type = file_type_from_filename(_filename);
  }
  switch (_type) {
    case FileType::Csv:
      table = CsvParser::parse(_filename, _chunk_size, _csv_meta);
      break;
     case FileType::Tbl:
      table = load_table(_filename, _chunk_size);
      break;
    case FileType::Binary:
      table = BinaryParser::parse(_filename);
      break;
    case FileType::Auto:
      Fail("File type should have been determined previously.");
  }

  if (_tablename) {
    Hyrise::get().storage_manager.add_table(*_tablename, table);
  }

  //return table;
  return nullptr;
}

std::shared_ptr<AbstractOperator> Import::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<Import>(_filename, _tablename, _chunk_size, _type, _csv_meta);
}

void Import::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

}  // namespace opossum
