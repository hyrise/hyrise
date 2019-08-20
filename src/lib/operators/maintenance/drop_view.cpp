#include "drop_view.hpp"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "hyrise.hpp"

namespace opossum {

DropView::DropView(const std::string& view_name, const bool if_exists)
    : AbstractReadOnlyOperator(OperatorType::DropView), view_name(view_name), if_exists(if_exists) {}

const std::string DropView::name() const { return "DropView"; }

std::shared_ptr<AbstractOperator> DropView::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<DropView>(view_name, if_exists);
}

void DropView::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

std::shared_ptr<const Table> DropView::_on_execute() {
  // If IF EXISTS is not set and the view is not found, StorageManager throws an exception
  if (!if_exists || Hyrise::get().storage_manager.has_view(view_name)) {
    Hyrise::get().storage_manager.drop_view(view_name);
  }

  return std::make_shared<Table>(TableColumnDefinitions{{"OK", DataType::Int, false}}, TableType::Data);  // Dummy table
}

}  // namespace opossum
