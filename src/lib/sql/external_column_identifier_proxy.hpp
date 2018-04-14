#pragma once

#include <memory>
#include <unordered_map>

#include "expression/expression_utils.hpp"

namespace opossum {

class AbstractExpression;
class ColumnIdentifierLookup;
class QualifiedColumnName;

/**
 * Exposes the Expressions of the outer query to an inner query during name resolution. Tracks which expression the
 * inner query is using
 */
class ExternalColumnIdentifierProxy final {
 public:
  using ExternalExpressions = std::unordered_map<std::shared_ptr<AbstractExpression>,
  std::shared_ptr<AbstractExpression>, ExpressionSharedPtrHash, ExpressionSharedPtrEquals>;

  explicit ExternalColumnIdentifierProxy(const std::shared_ptr<ColumnIdentifierLookup>& expression_lookup);

  std::shared_ptr<AbstractExpression> get(const QualifiedColumnName& qualified_column_name);

  const ExternalExpressions& referenced_external_expressions() const;

 private:
  std::shared_ptr<ColumnIdentifierLookup> _expression_lookup;
  ExternalExpressions _referenced_external_expressions;
};

}  // namespace opossum
