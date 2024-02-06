#include "drop_view.hpp"

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "all_type_variant.hpp"
#include "hyrise.hpp"
#include "operators/abstract_operator.hpp"
#include "operators/abstract_read_only_operator.hpp"
#include "operators/abstract_read_write_operator.hpp"
#include "storage/table.hpp"

namespace hyrise {

DropView::DropView(const std::string& init_view_name, const bool init_if_exists)
    : AbstractReadOnlyOperator(OperatorType::DropView), view_name(init_view_name), if_exists(init_if_exists) {}

const std::string& DropView::name() const {
  static const auto name = std::string{"DropView"};
  return name;
}

std::shared_ptr<AbstractOperator> DropView::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& /*copied_left_input*/,
    const std::shared_ptr<AbstractOperator>& /*copied_right_input*/,
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& /*copied_ops*/) const {
  return std::make_shared<DropView>(view_name, if_exists);
}

void DropView::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

std::shared_ptr<const Table> DropView::_on_execute() {
  // If IF EXISTS is not set and the view is not found, StorageManager throws an exception
  if (!if_exists || Hyrise::get().storage_manager.has_view(view_name)) {
    Hyrise::get().storage_manager.drop_view(view_name);
  }

  return nullptr;
}

}  // namespace hyrise
