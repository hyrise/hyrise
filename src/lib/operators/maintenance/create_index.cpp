#include "create_index.hpp"

#include <sstream>

#include "constant_mappings.hpp"
#include "hyrise.hpp"
#include "operators/insert.hpp"
#include "storage/table.hpp"
#include "storage/index/group_key/group_key_index.hpp"

namespace opossum {

CreateIndex::CreateIndex(const std::string& init_index_name,
                         const std::shared_ptr<const std::vector<ColumnID>>& init_column_ids,
                         const std::string& init_target_table_name,
                         const std::shared_ptr<const AbstractOperator>& input_operator)
    : AbstractReadWriteOperator(OperatorType::CreateIndex, input_operator),
      index_name(init_index_name),
      column_ids(init_column_ids),
      target_table_name(init_target_table_name)
    {}

const std::string& CreateIndex::name() const {
  static const auto name = std::string{"CreateIndex"};
  return name;
}

std::string CreateIndex::description(DescriptionMode description_mode) const {
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
  return std::string("create index description");
}

std::shared_ptr<const Table> CreateIndex::_on_execute(std::shared_ptr<TransactionContext> context) {
  auto table_to_be_indexed = Hyrise::get().storage_manager.get_table(target_table_name);

  table_to_be_indexed->create_index<GroupKeyIndex>(*column_ids, index_name);
  return std::make_shared<Table>(TableColumnDefinitions{{"OK", DataType::Int, false}}, TableType::Data);  // Dummy table
}

std::shared_ptr<AbstractOperator> CreateIndex::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_left_input,
    const std::shared_ptr<AbstractOperator>& copied_right_input,
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const {
  return std::make_shared<CreateIndex>(index_name, column_ids, target_table_name, copied_left_input);
}

void CreateIndex::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {
  // No parameters possible for CREATE TABLE
}

}  // namespace opossum
