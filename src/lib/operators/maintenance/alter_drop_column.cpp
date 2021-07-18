#include "alter_drop_column.hpp"

#include <sstream>

#include "constant_mappings.hpp"
#include "hyrise.hpp"
#include "operators/insert.hpp"
#include "storage/table.hpp"
#include "utils/assert.hpp"


namespace opossum {

AlterDropColumn::AlterDropColumn(const std::string& init_table_name, const std::string& init_column_name, const bool init_if_exists)
    : AbstractReadWriteOperator(OperatorType::AlterDropColumn),
      target_table_name(init_table_name), target_column_name(init_column_name),
      if_exists(init_if_exists)
    {}

const std::string& AlterDropColumn::name() const {
  static const auto name = std::string{"AlterDropColumn"};
  return name;
}

std::string AlterDropColumn::description(DescriptionMode description_mode) const {
  // TODO: craft usefull description print output
  /*
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
   */
  return std::string("Alter Drop Column description");
}

std::shared_ptr<const Table> AlterDropColumn::_on_execute(std::shared_ptr<TransactionContext> context) {
  if(if_exists) {
    if(_column_exists_on_table(target_table_name, target_column_name)) {
      Hyrise::get().storage_manager.drop_column_from_table(target_table_name, target_column_name);
    } else {
      std::cout << "Column " + target_column_name + " does not exist on Table " + target_table_name;
    }
  } else {
    Hyrise::get().storage_manager.drop_column_from_table(target_table_name, target_column_name);
  }

  return std::make_shared<Table>(TableColumnDefinitions{{"OK", DataType::Int, false}}, TableType::Data);  // Dummy table
}

std::shared_ptr<AbstractOperator> AlterDropColumn::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_left_input,
    const std::shared_ptr<AbstractOperator>& copied_right_input,
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const {
  return std::make_shared<AlterDropColumn>(target_table_name, target_column_name, if_exists);
}

void AlterDropColumn::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {
  // No parameters possible for CREATE TABLE
}

bool AlterDropColumn::_column_exists_on_table(const std::string& table_name, const std::string& column_name) {
  bool result = false;
  auto column_defs = Hyrise::get().storage_manager.get_table(table_name)->column_definitions();
  for(auto col_def : column_defs) {
    if(col_def.name == column_name) { result = true; }
  }
  return result;
}

}  // namespace opossum
