#include "table_wrapper.hpp"

#include <memory>
#include <string>
#include <vector>

namespace opossum {

TableWrapper::TableWrapper(const TableCSPtr table)
    : AbstractReadOnlyOperator(OperatorType::TableWrapper), _table(table) {}

const std::string TableWrapper::name() const { return "TableWrapper"; }

AbstractOperatorSPtr TableWrapper::_on_recreate(
    const std::vector<AllParameterVariant>& args, const AbstractOperatorSPtr& recreated_input_left,
    const AbstractOperatorSPtr& recreated_input_right) const {
  return std::make_shared<TableWrapper>(_table);
}

TableCSPtr TableWrapper::_on_execute() { return _table; }
}  // namespace opossum
