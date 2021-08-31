#include "drop_column_impl.hpp"
#include "abstract_alter_table_impl.hpp"
#include "hyrise.hpp"
#include "statistics/generate_pruning_statistics.hpp"
#include "statistics/table_statistics.hpp"

namespace opossum {

DropColumnImpl::DropColumnImpl(std::string init_table_name,
                               const std::shared_ptr<AbstractAlterTableAction> init_alter_action)
    : AbstractAlterTableImpl(init_table_name, init_alter_action) {
  drop_column_action = std::static_pointer_cast<DropColumnAction>(_action);
}

void DropColumnImpl::on_execute(std::shared_ptr<TransactionContext> context) {
  if (_column_exists_on_table(_target_table_name, drop_column_action->column_name)) {
    auto target_table = Hyrise::get().storage_manager.get_table(_target_table_name);
    auto indexes = target_table->indexes_statistics();
    auto target_column_id = target_table->column_id_by_name(drop_column_action->column_name);

    for (auto index : indexes) {
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
    if (drop_column_action->if_exists) {
      std::cout << "Column " + _target_table_name + " does not exist on Table " + _target_table_name;
    } else {
      throw std::logic_error("No such column");
    }
  }
}

std::string DropColumnImpl::description() {
  return std::string("DropColumn '" + drop_column_action->column_name + "'");
}

bool DropColumnImpl::_column_exists_on_table(const std::string& table_name, std::string& column_name) {
  auto column_defs = Hyrise::get().storage_manager.get_table(table_name)->column_definitions();
  for (auto column_def : column_defs) {
    if (column_def.name == column_name) {
      return true;
    }
  }
  return false;
}

}  // namespace opossum
