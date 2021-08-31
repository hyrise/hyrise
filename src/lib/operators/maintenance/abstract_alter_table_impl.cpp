#include "operators/abstract_read_write_operator.hpp"
#include "operators/insert.hpp"
#include "storage/table_column_definition.hpp"
#include "logical_query_plan/abstract_alter_table_action.hpp"
#include "abstract_alter_table_impl.hpp"

namespace opossum {

AbstractAlterTableImpl::AbstractAlterTableImpl(std::string init_table_name,
                                               const std::shared_ptr<AbstractAlterTableAction>& init_alter_action) : _target_table_name(init_table_name), _action(init_alter_action) {}
}  // namespace opossum
