#include "create_view.hpp"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "hyrise.hpp"
#include "storage/lqp_view.hpp"

namespace opossum {

CreateView::CreateView(const std::string& view_name, const std::shared_ptr<LQPView>& view, const bool if_not_exists)
    : AbstractReadOnlyOperator(OperatorType::CreateView),
      _view_name(view_name),
      _view(view),
      _if_not_exists(if_not_exists) {}

const std::string CreateView::name() const { return "CreateView"; }

const std::string& CreateView::view_name() const { return _view_name; }
bool CreateView::if_not_exists() const { return _if_not_exists; }

std::shared_ptr<AbstractOperator> CreateView::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<CreateView>(_view_name, _view->deep_copy(), _if_not_exists);
}

void CreateView::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

std::shared_ptr<const Table> CreateView::_on_execute() {
  // If IF NOT EXISTS is not set and the view already exists, StorageManager throws an exception
  if (!_if_not_exists || !Hyrise::get().storage_manager.has_view(_view_name)) {
    Hyrise::get().storage_manager.add_view(_view_name, _view);
  }
  return std::make_shared<Table>(TableColumnDefinitions{{"OK", DataType::Int, false}}, TableType::Data);  // Dummy table
}

}  // namespace opossum
