#include "drop_index.hpp"

#include <sstream>

#include "constant_mappings.hpp"
#include "hyrise.hpp"
#include "operators/insert.hpp"
#include "storage/table.hpp"
#include "storage/index/group_key/group_key_index.hpp"
#include "utils/assert.hpp"


namespace opossum {

DropIndex::DropIndex(const std::string& init_index_name, const bool init_if_exists, const std::string& init_table_name)
    : AbstractReadWriteOperator(OperatorType::DropIndex),
      index_name(init_index_name),
      if_exists(init_if_exists),
      table_name(init_table_name)
    {}

const std::string& DropIndex::name() const {
  static const auto name = std::string{"DropIndex"};
  return name;
}

std::string DropIndex::description(DescriptionMode description_mode) const {
  std::ostringstream stream;

  stream << AbstractOperator::description(description_mode);
  if(if_exists) stream << " 'IF EXISTS'";
  stream << " '" << index_name << "' ON";
  stream << " '" << table_name << "'";

  return stream.str();
}

std::shared_ptr<const Table> DropIndex::_on_execute(std::shared_ptr<TransactionContext> context) {
  auto table_to_be_indexed = Hyrise::get().storage_manager.get_table(table_name);

  table_to_be_indexed->delete_index_by_name(index_name, if_exists);

  return std::make_shared<Table>(TableColumnDefinitions{{"OK", DataType::Int, false}}, TableType::Data);  // Dummy table
}

std::shared_ptr<AbstractOperator> DropIndex::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_left_input,
    const std::shared_ptr<AbstractOperator>& copied_right_input,
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const {
  return std::make_shared<DropIndex>(index_name, if_exists, table_name);
}

void DropIndex::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {
  // No parameters possible for DROP INDEX
}

}  // namespace opossum
