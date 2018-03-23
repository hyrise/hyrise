#pragma once

#include <memory>
#include <optional>
#include <string>

namespace opossum {

class AliasedExpression final {
 public:
  AliasedExpression(const std::shared_ptr<AbstractExpression>& expression, const std::optional<std::string>& alias = std::nullopt):
    _expression(expression), _alias(alias) {}

  std::shared_ptr<AbstractExpression> expression;
  std::optional<std::string> alias;
};

}  // namespace opossum
