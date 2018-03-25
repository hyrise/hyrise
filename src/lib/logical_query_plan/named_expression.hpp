#pragma once

#include <memory>
#include <optional>
#include <string>

namespace opossum {

class NamedExpression final {
 public:
  NamedExpression(const std::shared_ptr<AbstractExpression>& expression, const std::optional<std::string>& alias = std::nullopt):
  expression(expression), alias(alias) {}

  std::shared_ptr<AbstractExpression> expression;
  std::optional<std::string> alias;
};

}  // namespace opossum
