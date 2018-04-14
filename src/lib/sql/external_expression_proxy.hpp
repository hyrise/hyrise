#pragma once

#include <memory>
#include <unordered_map>

#include "expression/expression_utils.hpp"

namespace opossum {

class AbstractExpression;
class QualifiedColumnNameLookup;
class QualifiedColumnName;

/**
 * Exposes the Expressions of the outer query to an inner query during name resolution. Tracks which expression the
 * inner query is using
 */
class ExternalExpressionProxy final {
 public:
  using ExternalExpressions = std::unordered_map<std::shared_ptr<AbstractExpression>,
  std::shared_ptr<AbstractExpression>, ExpressionSharedPtrHash, ExpressionSharedPtrEquals>;

  explicit ExternalExpressionProxy(const std::shared_ptr<QualifiedColumnNameLookup>& expression_lookup);

  std::shared_ptr<AbstractExpression> get(const QualifiedColumnName& qualified_column_name);

  const ExternalExpressions& referenced_external_expressions() const;

 private:
  std::shared_ptr<QualifiedColumnNameLookup> _expression_lookup;
  ExternalExpressions _referenced_external_expressions;
};

}  // namespace opossum
