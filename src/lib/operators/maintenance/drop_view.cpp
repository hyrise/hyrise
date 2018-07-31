#include "drop_view.hpp"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "storage/storage_manager.hpp"

namespace opossum {

DropView::DropView(const std::string& view_name)
    : AbstractReadOnlyOperator(OperatorType::DropView), _view_name(view_name) {}

const std::string DropView::name() const { return "DropView"; }

std::shared_ptr<AbstractOperator> DropView::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<DropView>(_view_name);
}

void DropView::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

std::shared_ptr<const Table> DropView::_on_execute() {
  StorageManager::get().drop_lqp_view(_view_name);

  return std::make_shared<Table>(TableColumnDefinitions{{"OK", DataType::Int}}, TableType::Data);  // Dummy table
}

}  // namespace opossum
