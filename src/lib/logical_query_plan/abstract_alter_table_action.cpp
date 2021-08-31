#include "abstract_alter_table_action.hpp"

namespace opossum {

AbstractAlterTableAction::AbstractAlterTableAction(hsql::AlterAction init_action) : action(init_action) {}
AbstractAlterTableAction::~AbstractAlterTableAction() {}
}  // namespace opossum
