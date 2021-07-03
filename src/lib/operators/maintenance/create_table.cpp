#include "create_table.hpp"

#include <sstream>

#include "constant_mappings.hpp"
#include "hyrise.hpp"
#include "operators/insert.hpp"
#include "storage/table.hpp"

namespace opossum {

CreateTable::CreateTable(const std::string& init_table_name, const bool init_if_not_exists,
                         const std::shared_ptr<const AbstractOperator>& input_operator,
                         std::shared_ptr<TableKeyConstraints> init_key_constraints)
    : AbstractReadWriteOperator(OperatorType::CreateTable, input_operator),
      table_name(init_table_name),
      if_not_exists(init_if_not_exists),
      key_constraints(init_key_constraints){}

const std::string& CreateTable::name() const {
  static const auto name = std::string{"CreateTable"};
  return name;
}

std::string CreateTable::description(DescriptionMode description_mode) const {
  std::ostringstream stream;

  const auto* const separator = description_mode == DescriptionMode::SingleLine ? ", " : "\n";

  // If the input operator has already been cleared, we cannot retrieve its columns anymore. However, since the table
  // has been created, we can simply pull the definitions from the new table.
  const auto column_definitions = left_input_table()
                                      ? left_input_table()->column_definitions()
                                      : Hyrise::get().storage_manager.get_table(table_name)->column_definitions();

  stream << AbstractOperator::description(description_mode) << " '" << table_name << "' (";
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
  return left_input_table()->column_definitions();
}

std::shared_ptr<const Table> CreateTable::_on_execute(std::shared_ptr<TransactionContext> context) {
  const auto column_definitions = _left_input->get_output()->column_definitions();

  // If IF NOT EXISTS is not set and the table already exists, StorageManager throws an exception
  if (!if_not_exists || !Hyrise::get().storage_manager.has_table(table_name)) {
    // TODO(anybody) chunk size and mvcc not yet specifiable
    const auto table = std::make_shared<Table>(column_definitions, TableType::Data, Chunk::DEFAULT_SIZE, UseMvcc::Yes);
    Hyrise::get().storage_manager.add_table(table_name, table);

    // Insert table data (if no data is present, insertion makes no difference)
    _insert = std::make_shared<Insert>(table_name, _left_input);
    _insert->set_transaction_context(context);
    _insert->execute();
  }
  auto table = std::make_shared<Table>(TableColumnDefinitions{{"OK", DataType::Int, false}}, TableType::Data);  // Dummy table

  if (key_constraints != nullptr ){
    for (const TableKeyConstraint& table_key_constraint : *key_constraints){
        table->Table::add_soft_key_constraint(table_key_constraint);
      }
  }

  return table;
}

std::shared_ptr<AbstractOperator> CreateTable::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_left_input,
    const std::shared_ptr<AbstractOperator>& copied_right_input,
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const {
  return std::make_shared<CreateTable>(table_name, if_not_exists, copied_left_input, key_constraints);
}

void CreateTable::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {
  // No parameters possible for CREATE TABLE
}

}  // namespace opossum
