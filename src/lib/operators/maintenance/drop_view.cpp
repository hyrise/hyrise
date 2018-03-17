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

std::shared_ptr<AbstractOperator> DropView::_on_recreate(
    const std::vector<AllParameterVariant>& args, const std::shared_ptr<AbstractOperator>& recreated_input_left,
    const std::shared_ptr<AbstractOperator>& recreated_input_right) const {
  Fail("This operator cannot be recreated");
  // ... because it makes no sense to do so.
}

std::shared_ptr<const Table> DropView::_on_execute() {
  StorageManager::get().drop_view(_view_name);

  return std::make_shared<Table>(TableColumnDefinitions{}, TableType::Data);  // Dummy table
}

}  // namespace opossum
