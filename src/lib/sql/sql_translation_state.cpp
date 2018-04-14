#include "sql_translation_state.hpp"

namespace opossum {

SQLTranslationState::SQLTranslationState(const std::shared_ptr<AbstractLQPNode>& lqp, const std::shared_ptr<ColumnIdentifierLookup>& column_identifier_lookup):
  lqp(lqp), column_identifier_lookup(column_identifier_lookup) {}

}  // namespace opossum
