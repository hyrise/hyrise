#include "import.hpp"

#include <cstdint>
#include <fstream>
#include <memory>
#include <numeric>
#include <utility>

#include <boost/algorithm/string.hpp>

#include "constant_mappings.hpp"
#include "hyrise.hpp"
#include "import_export/csv/csv_parser.hpp"
#include "import_export/binary/binary_parser.hpp"
#include "resolve_type.hpp"
#include "storage/chunk.hpp"
#include "storage/encoding_type.hpp"
#include "storage/vector_compression/fixed_size_byte_aligned/fixed_size_byte_aligned_vector.hpp"
#include "utils/assert.hpp"
#include "utils/load_table.hpp"

namespace opossum {

Import::Import(const std::string& file_name,
                const std::optional<std::string>& table_name,
                const std::optional<hsql::ImportType>& type,
                const ChunkOffset chunk_size,
                const std::optional<CsvMeta>& csv_meta)
    : AbstractReadOnlyOperator(OperatorType::Import), _file_name(file_name), _table_name(table_name), _type(type),
      _chunk_size(chunk_size), _csv_meta(csv_meta) {}

const std::string& Import::name() const {
  static const auto name = std::string{"Import"};
  return name;
}

std::shared_ptr<const Table> Import::_on_execute() {
  if (_table_name && Hyrise::get().storage_manager.has_table(*_table_name)) {
    return Hyrise::get().storage_manager.get_table(*_table_name);
  }

  // Check if file exists before giving it to the parser
  std::ifstream file(_file_name);
  Assert(file.is_open(), "Import: Could not find file " + _file_name);
  file.close();

  const auto table = _import();

  if (_table_name) {
    Hyrise::get().storage_manager.add_table(*_table_name, table);
  }

  return table;
}

std::shared_ptr<AbstractOperator> Import::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<Import>(_file_name, _table_name, _type);
}

void Import::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

std::shared_ptr<Table> Import::_import() {
  // if (!_type || *_type == hsql::ImportType::kImportAuto) {
  if (!_type) {
    return _import_any_file();
  } else {
    switch (*_type) {
      case hsql::ImportType::kImportCSV:
        return _import_csv(_file_name, _chunk_size, _csv_meta);
      case hsql::ImportType::kImportTbl:
        return _import_tbl(_file_name, _chunk_size);
      /*case hsql::ImportType::kImportBin:            // TODO: add bin type in statement
        return _import_binary(_file_name); */
      default:
        Fail("Cannot import file type.");
    }
  }
}

std::shared_ptr<Table> Import::_import_csv(const std::string& file_name, const ChunkOffset& chunk_size,
    const std::optional<CsvMeta>& csv_meta) {
  CsvParser parser;
  return parser.parse(file_name, csv_meta, chunk_size);
}

std::shared_ptr<Table> Import::_import_tbl(const std::string& file_name, const ChunkOffset& chunk_size) {
  return load_table(file_name, chunk_size);
}

std::shared_ptr<Table> Import::_import_binary(const std::string& file_name) {
  BinaryParser parser;
  return parser.parse(file_name);
}

std::shared_ptr<Table> Import::_import_any_file() {
  std::vector<std::string> file_parts;
  boost::algorithm::split(file_parts, _file_name, boost::is_any_of("."));
  const std::string& extension = file_parts.back();
  if (extension == "csv") {
    return _import_csv(_file_name, _chunk_size, _csv_meta);
  } else if (extension == "tbl") {
    return _import_tbl(_file_name, _chunk_size);
  } else if (extension == "bin") {
    return _import_binary(_file_name);
  }

  Fail("Cannot import file type.");
}

}  // namespace opossum
