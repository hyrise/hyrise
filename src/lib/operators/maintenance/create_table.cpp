#include "create_table.hpp"

#include <sstream>

#include "constant_mappings.hpp"
#include "hyrise.hpp"
#include "operators/insert.hpp"
#include "storage/table.hpp"

namespace opossum {

CreateTable::CreateTable(const std::string& table_name, const bool if_not_exists,
                         const std::shared_ptr<const AbstractOperator>& in)
    : AbstractReadWriteOperator(OperatorType::CreateTable, in), table_name(table_name), if_not_exists(if_not_exists) {}

const std::string& CreateTable::name() const {
  static const auto name = std::string{"CreateTable"};
  return name;
}

std::string CreateTable::description(DescriptionMode description_mode) const {
  std::ostringstream stream;

  const auto separator = description_mode == DescriptionMode::SingleLine ? ", " : "\n";

  const auto column_definitions = input_table_left()->column_definitions();

  stream << "CreateTable '" << table_name << "' (";
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

const TableColumnDefinitions& CreateTable::column_definitions() const {
  return input_table_left()->column_definitions();
}

std::shared_ptr<const Table> CreateTable::_on_execute(std::shared_ptr<TransactionContext> context) {
  const auto column_definitions = _input_left->get_output()->column_definitions();

  // If IF NOT EXISTS is not set and the table already exists, StorageManager throws an exception
  if (!if_not_exists || !Hyrise::get().storage_manager.has_table(table_name)) {
    // TODO(anybody) chunk size and mvcc not yet specifiable
    const auto table = std::make_shared<Table>(column_definitions, TableType::Data, Chunk::DEFAULT_SIZE, UseMvcc::Yes);
    Hyrise::get().storage_manager.add_table(table_name, table);

    // Insert table data (if no data is present, insertion makes no difference)
    _insert = std::make_shared<Insert>(table_name, _input_left);
    _insert->set_transaction_context(context);
    _insert->execute();
  }
  return std::make_shared<Table>(TableColumnDefinitions{{"OK", DataType::Int, false}}, TableType::Data);  // Dummy table
}

std::shared_ptr<AbstractOperator> CreateTable::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<CreateTable>(table_name, if_not_exists, _input_left);
}

void CreateTable::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {
  // No parameters possible for CREATE TABLE
}

}  // namespace opossum
