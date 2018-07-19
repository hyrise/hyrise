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
 * The SQLIdentifierResolverProxy tracks all outer expressions accessed in sub-Selects, so these can be turned into
 * parameters of the sub-Select.
 */
class SQLIdentifierResolverProxy final {
 public:
  SQLIdentifierResolverProxy(const std::shared_ptr<SQLIdentifierResolver>& wrapped_context,
                            const std::shared_ptr<ParameterIDAllocator>& parameter_id_allocator,
                            const std::shared_ptr<SQLIdentifierResolverProxy>& outer_context_proxy = {});

  std::shared_ptr<AbstractExpression> resolve_identifier_relaxed(const SQLIdentifier& identifier);

  const ExpressionUnorderedMap<ParameterID>& accessed_expressions() const;

 private:
  std::shared_ptr<SQLIdentifierResolver> _wrapped_context;
  std::shared_ptr<ParameterIDAllocator> _parameter_id_allocator;
  std::shared_ptr<SQLIdentifierResolverProxy> _outer_context_proxy;

  // Previously accessed expressions that were already assigned a ParameterID
  ExpressionUnorderedMap<ParameterID> _accessed_expressions;
};

}  // namespace opossum
