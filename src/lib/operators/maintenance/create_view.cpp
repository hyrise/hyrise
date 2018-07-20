#include "create_view.hpp"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "storage/lqp_view.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

CreateView::CreateView(const std::string& view_name, const std::shared_ptr<LQPView>& view)
    : AbstractReadOnlyOperator(OperatorType::CreateView), _view_name(view_name), _view(view) {}

const std::string CreateView::name() const { return "CreateView"; }

std::shared_ptr<AbstractOperator> CreateView::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<CreateView>(_view_name, _view->deep_copy());
}

void CreateView::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

std::shared_ptr<const Table> CreateView::_on_execute() {
  StorageManager::get().add_lqp_view(_view_name, _view);

  return std::make_shared<Table>(TableColumnDefinitions{{"OK", DataType::Int}}, TableType::Data);  // Dummy table
}

}  // namespace opossum
