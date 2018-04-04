#pragma once

#include <memory>
#include <optional>
#include <string>

namespace opossum {

class AbstractExpression;

struct PlanColumnDefinition final {
  explicit PlanColumnDefinition(const std::shared_ptr<AbstractExpression>& expression, const std::optional<std::string>& alias = std::nullopt);

  std::string description() const;

  std::shared_ptr<AbstractExpression> expression;
  std::optional<std::string> alias;
};

}  // namespace opossum
