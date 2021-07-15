#include "create_index.hpp"

#include <sstream>

#include "constant_mappings.hpp"
#include "hyrise.hpp"
#include "operators/insert.hpp"
#include "storage/table.hpp"
#include "storage/index/group_key/group_key_index.hpp"
#include "storage/index/group_key/composite_group_key_index.hpp"
#include "utils/assert.hpp"


namespace opossum {

CreateIndex::CreateIndex(const std::string& init_index_name,
                         const bool init_if_not_exists,
                         const std::string& init_table_name,
                         const std::shared_ptr<const std::vector<ColumnID>>& init_column_ids)
    : AbstractReadWriteOperator(OperatorType::CreateIndex),
      index_name(init_index_name),
      if_not_exists(init_if_not_exists),
      table_name(init_table_name),
      column_ids(init_column_ids)
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
  auto table_to_be_indexed = Hyrise::get().storage_manager.get_table(table_name);

  if(if_not_exists) {
    _check_if_index_already_exists(index_name, table_to_be_indexed);
  }

  // group key index only works with a single column
  if(column_ids->size() == 1) {
    table_to_be_indexed->create_index<GroupKeyIndex>(*column_ids, index_name);
  } else {
    table_to_be_indexed->create_index<CompositeGroupKeyIndex>(*column_ids, index_name);
  }

  return std::make_shared<Table>(TableColumnDefinitions{{"OK", DataType::Int, false}}, TableType::Data);  // Dummy table
}

std::shared_ptr<AbstractOperator> CreateIndex::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_left_input,
    const std::shared_ptr<AbstractOperator>& copied_right_input,
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const {
  return std::make_shared<CreateIndex>(index_name, if_not_exists, table_name, column_ids);
}

void CreateIndex::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {
  // No parameters possible for CREATE INDEX
}

void CreateIndex::_check_if_index_already_exists(std::string new_index_name, std::shared_ptr<Table> table) {
  auto index_statistics = table->indexes_statistics();
  for(auto index:index_statistics) {
    Assert(index.name != new_index_name, "Index " + new_index_name + " already exists");
  }
}

}  // namespace opossum
