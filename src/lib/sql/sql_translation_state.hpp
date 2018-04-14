#pragma once

#include <memory>

#include "logical_query_plan/qualified_column_name.hpp"

namespace opossum {

struct SQLTranslationState final {
  std::shared_ptr<AbstractLQPNode> lqp;
  std::shared_ptr<ColumnIdentifierLookup> qualified_column_name_lookup;
};

}  // namespace opossum
