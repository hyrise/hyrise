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
    {
  if(init_if_not_exists) {
    Assert(!init_index_name.empty(), "An index name must be provided when create index is called with if not exists");
  }
}

const std::string& CreateIndex::name() const {
  static const auto name = std::string{"CreateIndex"};
  return name;
}

std::string CreateIndex::description(DescriptionMode description_mode) const {

  std::ostringstream stream;

  stream << AbstractOperator::description(description_mode);
  if(if_not_exists) stream << " 'IF NOT EXISTS'";
  stream << " '" << index_name << "' ON";
  stream << " '" << table_name << "' column_ids(";
  for(auto column_id: *column_ids) {
    stream << "'" << column_id << "',";
  }

  stream << ")";

  return stream.str();
}

std::shared_ptr<const Table> CreateIndex::_on_execute(std::shared_ptr<TransactionContext> context) {
  auto table_to_be_indexed = Hyrise::get().storage_manager.get_table(table_name);

  auto index_exists = _index_already_exists(index_name, table_to_be_indexed);
  if(!index_exists) {
    // group key index only works with a single column
    if(column_ids->size() == 1) {
      table_to_be_indexed->create_index<GroupKeyIndex>(*column_ids, index_name);
    } else {
      table_to_be_indexed->create_index<CompositeGroupKeyIndex>(*column_ids, index_name);
    }
  }

  if(index_exists && !if_not_exists) {
    Fail("Index with name '" + index_name + "' already exists, no new index is created");
  }

  if(index_exists && if_not_exists) {
    std::cout << "Notice: Index with name '" + index_name + "' already exists, no new index is created.";
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

bool CreateIndex::_index_already_exists(std::string new_index_name, std::shared_ptr<Table> table) {
  auto index_statistics = table->indexes_statistics();

  for(auto index:index_statistics) {
    if(index.name == new_index_name) return true;
  }

  return false;
}

}  // namespace opossum
