#pragma once

#include <optional>

#include "abstract_expression.hpp"
#include "logical_query_plan/lqp_column_origin.hpp"

namespace opossum {

/**
 * Expression type used in LQPs, using LQPColumnOrigins to refer to Columns
 */
class LQPExpression : public AbstractExpression<LQPExpression> {
 public:
  static std::shared_ptr<LQPExpression> create_column(const LQPColumnOrigin& column_origin,
                                                      const std::optional<std::string>& alias = std::nullopt);

  static std::vector<std::shared_ptr<LQPExpression>> create_columns(
      const std::vector<LQPColumnOrigin>& column_origins,
      const std::optional<std::vector<std::string>>& aliases = std::nullopt);

  using AbstractExpression<LQPExpression>::AbstractExpression;

  const LQPColumnOrigin& column_origin() const;

  void set_column_origin(const LQPColumnOrigin& column_origin);

  std::string to_string(const std::optional<std::vector<std::string>>& input_column_names = std::nullopt,
                        bool is_root = true) const override;

  bool operator==(const LQPExpression& other) const;

 protected:
  void _deep_copy_impl(const std::shared_ptr<LQPExpression>& copy) const override;

 private:
  std::optional<LQPColumnOrigin> _column_origin;
};
}  // namespace opossum
