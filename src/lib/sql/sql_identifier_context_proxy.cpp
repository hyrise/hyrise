#include "sql_identifier_context_proxy.hpp"

#include "sql_identifier_context.hpp"

namespace opossum {

SQLIdentifierContextProxy::SQLIdentifierContextProxy(const std::shared_ptr<SQLIdentifierContext>& wrapped_context, const std::shared_ptr<SQLIdentifierContextProxy>& outer_context_proxy):
  _wrapped_context(wrapped_context), _outer_context_proxy(outer_context_proxy) {}

std::shared_ptr<AbstractExpression> SQLIdentifierContextProxy::resolve_identifier_relaxed(const SQLIdentifier& identifier) {
  auto expression = _wrapped_context->resolve_identifier_relaxed(identifier);
  if (expression) {
    const auto expression_iter = std::find_if(_accessed_expressions.begin(), _accessed_expressions.end(), [&](const auto& expression2) {
      return expression->deep_equals(*expression2);
    });

    if (expression_iter == _accessed_expressions.end()) {
      _accessed_expressions.emplace_back(expression);
    }
  } else {
    if (_outer_context_proxy) expression = _outer_context_proxy->resolve_identifier_relaxed(identifier);
  }

  return expression;
}

const std::vector<std::shared_ptr<AbstractExpression>>& SQLIdentifierContextProxy::accessed_expressions() const {
  return _accessed_expressions;
}

}  // namespace opossum
