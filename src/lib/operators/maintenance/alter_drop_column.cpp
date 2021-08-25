#include "alter_drop_column.hpp"

#include <sstream>

#include "constant_mappings.hpp"
#include "hyrise.hpp"
#include "operators/insert.hpp"
#include "storage/table.hpp"
#include "utils/assert.hpp"
#include "statistics/table_statistics.hpp"
#include "statistics/generate_pruning_statistics.hpp"


namespace opossum {

AlterDropColumn::AlterDropColumn(const std::string& init_table_name, const std::string& init_column_name, const bool init_if_exists)
    : AbstractReadWriteOperator(OperatorType::AlterTableDropColumn),
      target_table_name(init_table_name), target_column_name(init_column_name),
      if_exists(init_if_exists)
    {}

const std::string& AlterDropColumn::name() const {
  static const auto name = std::string{"AlterTableDropColumn"};
  return name;
}

std::string AlterDropColumn::description(DescriptionMode description_mode) const {
  std::ostringstream stream;
  stream << AbstractOperator::description(description_mode) << " '" + target_table_name << "'" << "('" << target_column_name << "')";
  return stream.str();
}

std::shared_ptr<const Table> AlterDropColumn::_on_execute(std::shared_ptr<TransactionContext> context) {

  if(_column_exists_on_table(target_table_name, target_column_name)) {
    auto target_table = Hyrise::get().storage_manager.get_table(target_table_name);
    auto indexes = target_table->indexes_statistics();
    auto target_column_id = target_table->column_id_by_name(target_column_name);

    for(auto index : indexes) {
      auto column_ids = index.column_ids;
      if (std::find(column_ids.begin(), column_ids.end(), target_column_id) != column_ids.end()) {
        // remove index if index is defined on column
        target_table->remove_index(index.name);
      }
    }

    target_table->delete_column(target_column_id);
    target_table->set_table_statistics(TableStatistics::from_table(*target_table));
    generate_chunk_pruning_statistics(target_table);

  } else {
    if(if_exists) {
      std::cout << "Column " + target_column_name + " does not exist on Table " + target_table_name;
    } else {
      throw "No such column";
    }
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
  // No parameters possible for ALTER DROP COLUMN
}

bool AlterDropColumn::_column_exists_on_table(const std::string& table_name, const std::string& column_name) {
  auto column_defs = Hyrise::get().storage_manager.get_table(table_name)->column_definitions();
  for(auto column_def : column_defs) {
    if(column_def.name == column_name) { return true; }
  }
  return false;
}

}  // namespace opossum
