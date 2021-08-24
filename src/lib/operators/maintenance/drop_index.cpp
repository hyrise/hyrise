#include "drop_index.hpp"

#include <sstream>

#include "constant_mappings.hpp"
#include "hyrise.hpp"
#include "operators/insert.hpp"
#include "storage/table.hpp"
#include "storage/index/group_key/group_key_index.hpp"
#include "utils/assert.hpp"


namespace opossum {

DropIndex::DropIndex(const std::string& init_index_name, const bool init_if_exists)
    : AbstractReadWriteOperator(OperatorType::DropIndex),
      index_name(init_index_name),
      if_exists(init_if_exists)
    {}

const std::string& DropIndex::name() const {
  static const auto name = std::string{"DropIndex"};
  return name;
}

std::string DropIndex::description(DescriptionMode description_mode) const {
  std::ostringstream stream;

  stream << AbstractOperator::description(description_mode);
  if(if_exists) stream << " 'IF EXISTS'";
  stream << " '" << index_name << "'";

  return stream.str();
}

std::shared_ptr<const Table> DropIndex::_on_execute(std::shared_ptr<TransactionContext> context) {
  auto tables = Hyrise::get().storage_manager.tables();
  auto deleted = false;

  for(auto table_pair : tables) {
    auto table = table_pair.second;
    for(auto index : table->indexes_statistics()) {
      if(index.name == index_name) {
        deleted = table->remove_index(index.name);
        // break nested loop
        goto end;
      }
    }
  }
  end:

  if(!if_exists) {
    Assert(deleted, "No index with name " + index_name);
  }

  return std::make_shared<Table>(TableColumnDefinitions{{"OK", DataType::Int, false}}, TableType::Data);  // Dummy table
}

std::shared_ptr<AbstractOperator> DropIndex::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_left_input,
    const std::shared_ptr<AbstractOperator>& copied_right_input,
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const {
  return std::make_shared<DropIndex>(index_name, if_exists);
}

void DropIndex::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {
  // No parameters possible for DROP INDEX
}

}  // namespace opossum
