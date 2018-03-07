#pragma once

#include <optional>

#include "abstract_expression.hpp"
#include "logical_query_plan/lqp_column_reference.hpp"

namespace opossum {

/**
 * Expression type used in LQPs, using LQPColumnReferences to refer to Columns.
 * AbstractExpression handles all other possible contained types (literals, operators, ...).
 */
class LQPExpression : public AbstractExpression<LQPExpression> {
 public:
  static std::shared_ptr<LQPExpression> create_column(const LQPColumnReference& column_reference,
                                                      const std::optional<std::string>& alias = std::nullopt);

  static std::vector<std::shared_ptr<LQPExpression>> create_columns(
      const std::vector<LQPColumnReference>& column_references,
      const std::optional<std::vector<std::string>>& aliases = std::nullopt);

  /**
   * Create an expression representing a subselect.
   * The expression will contain the root LQP node of the subselect.
   */
  static std::shared_ptr<LQPExpression> create_subselect(std::shared_ptr<AbstractLQPNode> root_node = nullptr,
                                                         const std::optional<std::string>& alias = std::nullopt);

  // Necessary for the AbstractExpression<T>::create_*() methods
  using AbstractExpression<LQPExpression>::AbstractExpression;

  const LQPColumnReference& column_reference() const;

  // Get the root LQP node of the contained subselect
  std::shared_ptr<AbstractLQPNode> subselect_node() const;

  void set_column_reference(const LQPColumnReference& column_reference);

  std::string to_string(const std::optional<std::vector<std::string>>& input_column_names = std::nullopt,
                        bool is_root = true) const override;

  bool operator==(const LQPExpression& other) const;

 protected:
  void _deep_copy_impl(const std::shared_ptr<LQPExpression>& copy) const override;

 private:
  std::optional<LQPColumnReference> _column_reference;
  std::shared_ptr<AbstractLQPNode> _subselect_node;
};
}  // namespace opossum
