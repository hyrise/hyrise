#include "create_view.hpp"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "storage/storage_manager.hpp"

namespace opossum {

CreateView::CreateView(const std::string& view_name, AbstractLQPNodeCSPtr lqp)
    : AbstractReadOnlyOperator(OperatorType::CreateView), _view_name(view_name), _lqp(lqp) {}

const std::string CreateView::name() const { return "CreateView"; }

AbstractOperatorSPtr CreateView::_on_recreate(
    const std::vector<AllParameterVariant>& args, const AbstractOperatorSPtr& recreated_input_left,
    const AbstractOperatorSPtr& recreated_input_right) const {
  return std::make_shared<CreateView>(_view_name, _lqp->deep_copy());
}

TableCSPtr CreateView::_on_execute() {
  StorageManager::get().add_view(_view_name, _lqp);

  return std::make_shared<Table>(TableColumnDefinitions{}, TableType::Data);
}

}  // namespace opossum
