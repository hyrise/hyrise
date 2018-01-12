#include "create_view.hpp"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "storage/storage_manager.hpp"

namespace opossum {

CreateView::CreateView(const std::string& view_name, std::shared_ptr<const AbstractLQPNode> lqp)
    : _view_name(view_name), _lqp(lqp) {}

const std::string CreateView::name() const { return "CreateView"; }

std::shared_ptr<AbstractOperator> CreateView::recreate(const std::vector<AllParameterVariant>& args) const {
  Fail("This operator cannot be recreated");
  // ... because it makes no sense to do so.
}

std::shared_ptr<const Table> CreateView::_on_execute() {
  StorageManager::get().add_view(_view_name, _lqp);

  return std::make_shared<Table>();
}

}  // namespace opossum
