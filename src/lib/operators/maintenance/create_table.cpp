#include "create_table.hpp"

#include <sstream>

#include "constant_mappings.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"

namespace opossum {

CreateTable::CreateTable(const std::string& table_name, const TableColumnDefinitions& column_definitions)
    : AbstractReadOnlyOperator(OperatorType::CreateTable),
      table_name(table_name),
      column_definitions(column_definitions) {}

const std::string CreateTable::name() const { return "Create Table"; }

const std::string CreateTable::description(DescriptionMode description_mode) const {
  std::ostringstream stream;

  const auto separator = description_mode == DescriptionMode::SingleLine ? ", " : "\n";

  stream << "Create Table '" << table_name << "' (";
  for (auto column_id = ColumnID{0}; column_id < column_definitions.size(); ++column_id) {
    const auto& column_definition = column_definitions[column_id];

    stream << "'" << column_definition.name << "' " << data_type_to_string.left.at(column_definition.data_type) << " ";
    if (column_definition.nullable) {
      stream << "NULL";
    } else {
      stream << "NOT NULL";
    }

    if (column_id + 1u < column_definitions.size()) {
      stream << separator;
    }
  }
  stream << ")";

  return stream.str();
}

std::shared_ptr<const Table> CreateTable::_on_execute() {
  // TODO(anybody) chunk size and mvcc not yet specifiable
  const auto table = std::make_shared<Table>(column_definitions, TableType::Data, Chunk::MAX_SIZE, UseMvcc::Yes);

  StorageManager::get().add_table(table_name, table);

  return std::make_shared<Table>(TableColumnDefinitions{{"OK", DataType::Int}}, TableType::Data);  // Dummy table
}

std::shared_ptr<AbstractOperator> CreateTable::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<CreateTable>(table_name, column_definitions);
}

void CreateTable::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {
  // No parameters possible for CREATE TABLE
}

}  // namespace opossum
