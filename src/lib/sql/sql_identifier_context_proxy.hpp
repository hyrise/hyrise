#pragma once

#include <memory>
#include <vector>

namespace opossum {

class AbstractExpression;
struct SQLIdentifier;
class SQLIdentifierContext;

class SQLIdentifierContextProxy final {
 public:
  explicit SQLIdentifierContextProxy(const std::shared_ptr<SQLIdentifierContext>& wrapped_context, const std::shared_ptr<SQLIdentifierContextProxy>& outer_context_proxy = {});

  std::shared_ptr<AbstractExpression> resolve_identifier_relaxed(const SQLIdentifier& identifier);

  const std::vector<std::shared_ptr<AbstractExpression>>& accessed_expressions() const;

 private:
  std::shared_ptr<SQLIdentifierContext> _wrapped_context;
  std::shared_ptr<SQLIdentifierContextProxy> _outer_context_proxy;
  std::vector<std::shared_ptr<AbstractExpression>> _accessed_expressions;
};


}  // namespace opossum