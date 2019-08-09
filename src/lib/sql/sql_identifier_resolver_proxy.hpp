#pragma once

#include <memory>
#include <vector>

#include "expression/abstract_expression.hpp"

namespace opossum {

class AbstractExpression;
struct SQLIdentifier;
class SQLIdentifierResolver;
class ParameterIDAllocator;

/**
 * Used during SQLTranslation to resolve identifiers from outer SELECTs in sub-SELECTs.
 * The SQLIdentifierResolverProxy provides and tracks access to a SELECT statement's identifiers from ANY inner
 *   query, no matter how deeply nested.
 *
 * Each nested SELECT is translated by a separate instance of the SQLTranslator. Each SQLTranslator has a
 *   SQLIdentifierResolver to resolve identifiers from its own SELECT-statement and an optional
 *   SQLIdentifierResolverProxy to resolve expressions from any parent SELECT.
 *
 * Consider "SELECT (SELECT t1.a + b FROM t2) FROM t1". The SQLTranslator uses the SQLIdentifierResolverProxy to resolve
 *   "t1.a", since "t1.a" cannot be resolved using its own SQLIdentifierResolver.
 *
 * The ParameterIDAllocator is global to the entire statement translation and makes sure ParameterIDs are unique
 *   across sub queries
 *
 * To be able to access expressions from SELECT-statements above the direct parent statement, the outer_context_proxy
 *   is used. If such a parent-parent query exists, `outer_context_proxy` points to its context. This nesting continues
 *   to any expression from any outer query can be accessed from any inner query and accesses to expressions from outer
 *   queries are tracked by the correct SQLIdentifierResolverProxy.
 */
class SQLIdentifierResolverProxy final {
 public:
  SQLIdentifierResolverProxy(const std::shared_ptr<SQLIdentifierResolver>& wrapped_resolver,
                             const std::shared_ptr<ParameterIDAllocator>& parameter_id_allocator,
                             const std::shared_ptr<SQLIdentifierResolverProxy>& outer_context_proxy = {});

  std::shared_ptr<AbstractExpression> resolve_identifier_relaxed(const SQLIdentifier& identifier);

  const ExpressionUnorderedMap<ParameterID>& accessed_expressions() const;

 private:
  std::shared_ptr<SQLIdentifierResolver> _wrapped_resolver;
  std::shared_ptr<ParameterIDAllocator> _parameter_id_allocator;
  std::shared_ptr<SQLIdentifierResolverProxy> _outer_context_proxy;

  // Previously accessed expressions that were already assigned a ParameterID
  ExpressionUnorderedMap<ParameterID> _accessed_expressions;
};

}  // namespace opossum
