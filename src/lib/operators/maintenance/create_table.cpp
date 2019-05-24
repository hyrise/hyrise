#include "create_table.hpp"

#include <sstream>

#include "concurrency/transaction_manager.hpp"
#include "constant_mappings.hpp"
#include "operators/insert.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"

namespace opossum {

CreateTable::CreateTable(const std::string& table_name, const TableColumnDefinitions& column_definitions,
                         const bool if_not_exists, const std::shared_ptr<AbstractOperator> in)
    : AbstractReadOnlyOperator(OperatorType::CreateTable, in),
      table_name(table_name),
      column_definitions(column_definitions),
      if_not_exists(if_not_exists) {}

const std::string CreateTable::name() const { return "Create Table"; }

const std::string CreateTable::description(DescriptionMode description_mode) const {
  std::ostringstream stream;

  const auto separator = description_mode == DescriptionMode::SingleLine ? ", " : "\n";

  stream << "Create Table '" << table_name << "' (";
  for (auto column_id = ColumnID{0}; column_id < column_definitions.size(); ++column_id) {
    const auto& column_definition = column_definitions[column_id];

    stream << "'" << column_definition.name << "' " << column_definition.data_type << " ";
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
  if (input_left()) {
    const auto input_table = input_table_left();
    //TODO if not exists
    //TODO only use column definitions of referenced columns
    const auto table = std::make_shared<Table>(column_definitions, TableType::Data, Chunk::DEFAULT_SIZE, UseMvcc::Yes);
    StorageManager::get().add_table(table_name, table);

    const auto insert = std::make_shared<Insert>(table_name, input_left());
    const auto context = TransactionManager::get().new_transaction_context();

    insert->set_transaction_context(context);
    insert->execute();
    context->commit();
  } else {
    // If IF NOT EXISTS is not set and the table already exists, StorageManager throws an exception
    if (!if_not_exists || !StorageManager::get().has_table(table_name)) {
      // TODO(anybody) chunk size and mvcc not yet specifiable
      const auto table = std::make_shared<Table>(column_definitions, TableType::Data, Chunk::DEFAULT_SIZE, UseMvcc::Yes);
      StorageManager::get().add_table(table_name, table);
    }
  }
  return std::make_shared<Table>(TableColumnDefinitions{{"OK", DataType::Int}}, TableType::Data);  // Dummy table
}

std::shared_ptr<AbstractOperator> CreateTable::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<CreateTable>(table_name, column_definitions, if_not_exists);
}

void CreateTable::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {
  // No parameters possible for CREATE TABLE
}

}  // namespace opossum
