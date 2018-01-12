#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "abstract_lqp_node.hpp"

namespace opossum {

class LQPExpression;
struct ColumnID;

/**
 * Node type to represent common projections, i.e. without any aggregate functionality.
 * It is, however, responsible to calculate arithmetic expressions.
 */
class ProjectionNode : public AbstractLQPNode {
 public:
  explicit ProjectionNode(const std::vector<std::shared_ptr<LQPExpression>>& column_expressions);

  const std::vector<std::shared_ptr<LQPExpression>>& column_expressions() const;

  std::string description() const override;

  const std::vector<LQPColumnReference>& output_column_references() const override;
  const std::vector<std::string>& output_column_names() const override;

  std::string get_verbose_column_name(ColumnID column_id) const override;

 protected:
  std::shared_ptr<AbstractLQPNode> _deep_copy_impl(
      const std::shared_ptr<AbstractLQPNode>& copied_left_child,
      const std::shared_ptr<AbstractLQPNode>& copied_right_child) const override;
  void _on_child_changed() override;

 private:
  std::vector<std::shared_ptr<LQPExpression>> _column_expressions;

  mutable std::optional<std::vector<std::string>> _output_column_names;

  void _update_output() const;
};

}  // namespace opossum
