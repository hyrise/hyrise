#include "column_origin.hpp"

#include "abstract_lqp_node.hpp"

namespace opossum {

std::string ColumnOrigin::get_verbose_name() const {
  DebugAssert(node && column_id != INVALID_COLUMN_ID);
  return node->get_verbose_name(column_id);
}

}