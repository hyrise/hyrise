#include "import.hpp"

#include <cstdint>
#include <fstream>
#include <memory>
#include <numeric>
#include <optional>
#include <string>
#include <utility>

#include <boost/algorithm/string.hpp>

#include "constant_mappings.hpp"
#include "hyrise.hpp"
#include "resolve_type.hpp"
#include "storage/chunk.hpp"
#include "storage/encoding_type.hpp"
#include "storage/vector_compression/fixed_size_byte_aligned/fixed_size_byte_aligned_vector.hpp"
#include "utils/assert.hpp"

namespace opossum {

Import::Import(const std::string& file_name,
                const std::optional<std::string>& table_name,
                const std::optional<hsql::ImportType>& type)
    : AbstractReadOnlyOperator(OperatorType::Import), _file_name(file_name), _table_name(table_name), _type(type) {}

const std::string& Import::name() const {
  static const auto name = std::string{"Import"};
  return name;
}

std::shared_ptr<const Table> Import::_on_execute() {
  if (_table_name && Hyrise::get().storage_manager.has_table(*_table_name)) {
    return Hyrise::get().storage_manager.get_table(*_table_name);
  }

  const auto table = _import(_file_name, _table_name, _type);

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

std::shared_ptr<Table> Import::_import(const std::string& file_name,
                                        const std::optional<std::string>& table_name,
                                        const std::optional<hsql::ImportType>& type) {
  // if (!_type || *_type == hsql::ImportType::kImportAuto) {
  if (!type) {
    return _import_any_file(file_name, *table_name);
  } else {
    switch (*type) {
      case hsql::ImportType::kImportCSV:
        return _import_csv(file_name, *table_name);
      case hsql::ImportType::kImportTbl:
        return _import_tbl(file_name, *table_name);
      /*case hsql::ImportType::kImportBin:            // TODO: add bin type in statement
        return _import_binary(file_name, *table_name); */
      default:
        Fail("Cannot import file type.");
    }
  }
}

std::shared_ptr<Table> Import::_import_csv(const std::string& file_name, const std::string& table_name) {
  return nullptr;
}

std::shared_ptr<Table> Import::_import_tbl(const std::string& file_name, const std::string& table_name) {
  return nullptr;
}

std::shared_ptr<Table> Import::_import_binary(const std::string& file_name, const std::string& table_name) {
  return nullptr;
}

std::shared_ptr<Table> Import::_import_any_file(const std::string& file_name, const std::string& table_name) {
  std::vector<std::string> file_parts;
  boost::algorithm::split(file_parts, file_name, boost::is_any_of("."));
  const std::string& extension = file_parts.back();
  if (extension == "csv") {
    return _import_csv(file_name, table_name);
  } else if (extension == "tbl") {
    return _import_tbl(file_name, table_name);
  } else if (extension == "bin") {
    return _import_binary(file_name, table_name);
  }

  Fail("Cannot import file type.");
}

}  // namespace opossum
