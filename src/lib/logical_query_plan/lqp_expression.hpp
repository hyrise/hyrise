#pragma once

#include <optional>

#include "expression.hpp"
#include "logical_query_plan/column_origin.hpp"

namespace opossum {

class LQPExpression : public Expression<LQPExpression> {
 public:
  static std::shared_ptr<LQPExpression> create_column(const ColumnOrigin& column_origin,
                                                      const std::optional<std::string>& alias = std::nullopt);

  static std::vector<std::shared_ptr<LQPExpression>> create_columns(
      const std::vector<ColumnOrigin>& column_origins,
      const std::optional<std::vector<std::string>>& aliases = std::nullopt);

  using Expression<LQPExpression>::Expression;

  const ColumnOrigin& column_origin() const;

  void set_column_origin(const ColumnOrigin& column_origin);

  std::string to_string(const std::optional<std::vector<std::string>>& input_column_names = std::nullopt,
                        bool is_root = true) const override;

  bool operator==(const LQPExpression& other) const;

 protected:
  void _deep_copy_impl(const std::shared_ptr<LQPExpression> &copy) const override;

 private:
  std::optional<ColumnOrigin> _column_origin;
};
}