#include "sql_identifier_context_proxy.hpp"

#include "expression/external_expression.hpp"
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

    auto value_placeholder_idx = uint16_t{0};
    if (expression_iter == _accessed_expressions.end()) {
      _accessed_expressions.emplace_back(expression);
      value_placeholder_idx = static_cast<uint16_t>(_accessed_expressions.size() - 1);
    } else {
      value_placeholder_idx = static_cast<uint16_t>(std::distance(_accessed_expressions.begin(), expression_iter));
    }

    return std::make_shared<ExternalExpression>(ValuePlaceholder{value_placeholder_idx}, expression->data_type(), expression->is_nullable(), expression->as_column_name());
  } else {
    Assert(!_outer_context_proxy, "More than one level of nesting not supported yet");
    return nullptr;
    // if (_outer_context_proxy) expression = _outer_context_proxy->resolve_identifier_relaxed(identifier);
  }
}

const std::vector<std::shared_ptr<AbstractExpression>>& SQLIdentifierContextProxy::accessed_expressions() const {
  return _accessed_expressions;
}

}  // namespace opossum
