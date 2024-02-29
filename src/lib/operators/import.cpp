#include "import.hpp"

#include <fstream>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>

#include "all_type_variant.hpp"
#include "hyrise.hpp"
#include "import_export/binary/binary_parser.hpp"
#include "import_export/csv/csv_meta.hpp"
#include "import_export/csv/csv_parser.hpp"
#include "import_export/file_type.hpp"
#include "operators/abstract_operator.hpp"
#include "operators/abstract_read_only_operator.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/load_table.hpp"

namespace hyrise {

Import::Import(const std::string& init_filename, const std::string& tablename, const ChunkOffset chunk_size,
               const FileType file_type, const std::optional<CsvMeta>& csv_meta)
    : AbstractReadOnlyOperator(OperatorType::Import),
      filename(init_filename),
      _tablename(tablename),
      _chunk_size(chunk_size),
      _file_type(file_type),
      _csv_meta(csv_meta) {
  if (_file_type == FileType::Auto) {
    _file_type = file_type_from_filename(filename);
  }
}

const std::string& Import::name() const {
  static const auto name = std::string{"Import"};
  return name;
}

std::shared_ptr<const Table> Import::_on_execute() {
  // Check if file exists before giving it to the parser
  auto file = std::ifstream{filename};
  Assert(file.is_open(), "Import: Could not find file " + filename);
  file.close();

  auto table = std::shared_ptr<Table>{};

  switch (_file_type) {
    case FileType::Csv:
      table = CsvParser::parse(filename, _chunk_size, _csv_meta);
      break;
    case FileType::Tbl:
      table = load_table(filename, _chunk_size);
      break;
    case FileType::Binary:
      table = BinaryParser::parse(filename);
      break;
    case FileType::Auto:
      Fail("File type should have been determined previously.");
  }

  if (Hyrise::get().storage_manager.has_table(_tablename)) {
    Hyrise::get().storage_manager.drop_table(_tablename);
  }

  Hyrise::get().storage_manager.add_table(_tablename, table);

  // must match ImportNode::output_expressions
  return nullptr;
}

std::shared_ptr<AbstractOperator> Import::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& /*copied_left_input*/,
    const std::shared_ptr<AbstractOperator>& /*copied_right_input*/,
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& /*copied_ops*/) const {
  return std::make_shared<Import>(filename, _tablename, _chunk_size, _file_type, _csv_meta);
}

void Import::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

}  // namespace hyrise
