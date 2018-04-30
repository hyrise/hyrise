#include "create_view.hpp"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "storage/storage_manager.hpp"

namespace opossum {

CreateView::CreateView(const std::string& view_name, std::shared_ptr<const AbstractLQPNode> lqp)
    : AbstractReadOnlyOperator(OperatorType::CreateView), _view_name(view_name), _lqp(lqp) {}

const std::string CreateView::name() const { return "CreateView"; }

std::shared_ptr<AbstractOperator> CreateView::_on_recreate(
    const std::vector<AllParameterVariant>& args, const std::shared_ptr<AbstractOperator>& recreated_input_left,
    const std::shared_ptr<AbstractOperator>& recreated_input_right) const {
  return std::make_shared<CreateView>(_view_name, _lqp->deep_copy());
}

std::shared_ptr<const Table> CreateView::_on_execute() {
  StorageManager::get().add_view(_view_name, _lqp);

  return std::make_shared<Table>(TableColumnDefinitions{{"OK", DataType::Int}}, TableType::Data);  // Dummy table
}

}  // namespace opossum
