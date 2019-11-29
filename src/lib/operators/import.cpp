#include "import.hpp"

#include <cstdint>
#include <fstream>
#include <memory>
#include <numeric>
#include <optional>
#include <string>
#include <utility>

#include "constant_mappings.hpp"
#include "hyrise.hpp"
#include "resolve_type.hpp"
#include "storage/chunk.hpp"
#include "storage/encoding_type.hpp"
#include "storage/vector_compression/fixed_size_byte_aligned/fixed_size_byte_aligned_vector.hpp"
#include "utils/assert.hpp"

namespace opossum {

Import::Import(const std::string& filename, const std::string& tablename, const hsql::ImportType type)
    : AbstractReadOnlyOperator(OperatorType::Import), _filename(filename), _tablename(tablename), _type(type) {}

const std::string& Import::name() const {
  static const auto name = std::string{"Import"};
  return name;
}

std::shared_ptr<const Table> Import::_on_execute() {
  /*if (_tablename && Hyrise::get().storage_manager.has_table(*_tablename)) {
    return Hyrise::get().storage_manager.get_table(*_tablename);
  }

  const auto table = read_binary(_filename);

  if (_tablename) {
    Hyrise::get().storage_manager.add_table(*_tablename, table);
  }

  return table; */
  return nullptr;
}

std::shared_ptr<AbstractOperator> Import::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<Import>(_filename, _tablename, _type);
}

void Import::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}


}  // namespace opossum
