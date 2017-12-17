#pragma once

#include <memory>

#include "types.hpp"

namespace opossum {

class AbstractLQPNode;

struct ColumnOrigin {
  ColumnOrigin(const std::shared_ptr<const AbstractLQPNode>& node, ColumnID column_id = INVALID_COLUMN_ID);

  bool operator==(const ColumnOrigin& rhs) const;

  std::shared_ptr<const AbstractLQPNode> node;
  ColumnID column_id = INVALID_COLUMN_ID;

  std::string get_verbose_name() const;
};

std::ostream& operator<<(std::ostream& os, const ColumnOrigin& column_origin);

}