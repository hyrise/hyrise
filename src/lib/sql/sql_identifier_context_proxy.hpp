//#pragma once
//
//#include <memory>
//#include <unordered_map>
//
//#include "expression/expression_utils.hpp"
//
//namespace opossum {
//
//class AbstractExpression;
//class ExternalExpression;
//class ColumnIdentifierLookup;
//class SQLIdentifier;
//
///**
// * Exposes the Expressions of the outer query to an inner query during name resolution. Tracks which expression the
// * inner query is using
// */
//class ColumnIdentifierLookupProxy final {
// public:
//  explicit ColumnIdentifierLookupProxy(const std::shared_ptr<ColumnIdentifierLookup>& column_identifier_lookup);
//
//  std::shared_ptr<AbstractExpression> resolve_identifier(const SQLIdentifier& column_identifier);
//
//  const std::vector<std::shared_ptr<ExternalExpression>>& referenced_external_expressions() const;
//
// private:
//  std::shared_ptr<ColumnIdentifierLookup>& _column_identifier_lookup;
//  std::vector<std::shared_ptr<ExternalExpression>> _external_expressions;
//};
//
//}  // namespace opossum
