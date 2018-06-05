#include "view.hpp"

namespace opossum {

View::View(const std::shared_ptr<AbstractLQPNode>& lqp,
                       const std::unordered_map<ColumnID, std::string>& column_names):
  lqp(lqp),
  column_names(column_names) {}

}  // namespace opossum
