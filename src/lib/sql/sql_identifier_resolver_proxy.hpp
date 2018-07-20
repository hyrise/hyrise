#pragma once

#include <memory>
#include <vector>

#include "expression/parameter_expression.hpp"

namespace opossum {

class AbstractExpression;
struct SQLIdentifier;
class SQLIdentifierResolver;
class ParameterIDAllocator;

/**
 * Used during SQLTranslation to resolve "outer expressions" in sub-Selects.
 * Consider "SELECT (SELECT t1.a + b FROM t2) FROM t2". The SQLTranslator uses the SQLIdentifierResolverProxy to resolve
 * "t1.a" while translating the sub-Select.
 *
 * -> The SQLIdentifierResolverProxy wraps the SQLIdentifierResolver of the parent query and can thus access expressions
 *    from the outer query. Every access to an expression from an outer query is tracked, so the outer query can bind
 *    these expressions as parameters to the sub-Select.
 *
 * -> The ParameterIDAllocator is global to the entire statement translation and makes sure ParamterIDs are unique
 *    across sub queries
 *
 * -> To be able to access expressions from any parent of the parent query, the `outer_context_proxy` is used.
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
