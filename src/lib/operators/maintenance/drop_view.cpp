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

AbstractOperatorSPtr DropView::_on_recreate(
    const std::vector<AllParameterVariant>& args, const AbstractOperatorSPtr& recreated_input_left,
    const AbstractOperatorSPtr& recreated_input_right) const {
  return std::make_shared<DropView>(_view_name);
}

TableCSPtr DropView::_on_execute() {
  StorageManager::get().drop_view(_view_name);

  return std::make_shared<Table>(TableColumnDefinitions{}, TableType::Data);  // Dummy table
}

}  // namespace opossum
