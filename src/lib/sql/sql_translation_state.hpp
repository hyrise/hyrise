#pragma once

#include <memory>

namespace opossum {

class AbstractLQPNode;
class ColumnIdentifierLookup;

struct SQLTranslationState final {
  SQLTranslationState(const std::shared_ptr<AbstractLQPNode>& lqp, const std::shared_ptr<ColumnIdentifierLookup>& column_identifier_lookup);

  std::shared_ptr<AbstractLQPNode> lqp;
  std::shared_ptr<ColumnIdentifierLookup> column_identifier_lookup;
};

}  // namespace opossum
