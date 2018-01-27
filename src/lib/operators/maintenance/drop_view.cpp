#include "drop_view.hpp"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "storage/storage_manager.hpp"

namespace opossum {

DropView::DropView(const std::string& view_name) : _view_name(view_name) {}

const std::string DropView::name() const { return "DropView"; }

std::shared_ptr<AbstractOperator> DropView::recreate(const std::vector<AllParameterVariant>& args) const {
  Fail("This operator cannot be recreated");
  // ... because it makes no sense to do so.
}

std::shared_ptr<const Table> DropView::_on_execute() {
  StorageManager::get().drop_view(_view_name);

  return std::make_shared<Table>();
}

}  // namespace opossum
