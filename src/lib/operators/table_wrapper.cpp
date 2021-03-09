#include "table_wrapper.hpp"

#include <memory>
#include <string>
#include <vector>

namespace opossum {

TableWrapper::TableWrapper(const std::shared_ptr<const Table>& init_table)
    : AbstractReadOnlyOperator(OperatorType::TableWrapper), table(init_table) {}

const std::string& TableWrapper::name() const {
  static const auto name = std::string{"TableWrapper"};
  return name;
}

std::shared_ptr<AbstractOperator> TableWrapper::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_left_input,
    const std::shared_ptr<AbstractOperator>& copied_right_input,
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const {
  return std::make_shared<TableWrapper>(table);
}

void TableWrapper::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

std::shared_ptr<const Table> TableWrapper::_on_execute() { return table; }
}  // namespace opossum
