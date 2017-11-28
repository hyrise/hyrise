#pragma once

#include <memory>

#include "types.hpp"

namespace opossum {

class AbstractLQPNode;

struct ColumnOrigin {
  std::shared_ptr<AbstractLQPNode> node;
  ColumnID column_id = INVALID_COLUMN_ID;

  std::string get_verbose_name() const;
};

}