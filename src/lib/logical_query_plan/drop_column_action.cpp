#include "abstract_non_query_node.hpp"
#include "enable_make_for_lqp_node.hpp"
#include "drop_column_action.hpp"

namespace opossum {

DropColumnAction::DropColumnAction(const hsql::DropColumnAction init_alter_action) : AbstractAlterTableAction(init_alter_action), drop_column_action(init_alter_action) {
  column_name = init_alter_action.columnName;
  if_exists = init_alter_action.ifExists;
  type = init_alter_action.type;
}

size_t DropColumnAction::on_shallow_hash(){
  auto hash = boost::hash_value(type);
  boost::hash_combine(hash, if_exists);
  boost::hash_combine(hash, column_name);
  return hash;
}

std::string DropColumnAction::description() {
  std::ostringstream stream;
  stream << "[DropColumn] " << (if_exists ? "(if exists) " : "");
  stream << "column: '" << column_name << "'";
  return stream.str();
}

std::shared_ptr<AbstractAlterTableAction> DropColumnAction::on_shallow_copy() {
  return std::make_shared<DropColumnAction>(drop_column_action);
}

bool DropColumnAction::on_shallow_equals(AbstractAlterTableAction& rhs) {
  if(action.type == rhs.type) {
    auto& concrete_action = static_cast<DropColumnAction&>(rhs);
    return if_exists == concrete_action.if_exists && column_name == concrete_action.column_name;
  } else {
      return false;
  }
}

}  // namespace opossum
