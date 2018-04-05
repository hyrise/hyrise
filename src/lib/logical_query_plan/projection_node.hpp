#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "abstract_lqp_node.hpp"
#include "utils/create_ptr_aliases.hpp"

namespace opossum {

class LQPExpression;
struct ColumnID;

/**
 * Node type to represent common projections, i.e. without any aggregate functionality.
 * It is, however, responsible to calculate arithmetic expressions.
 */
class ProjectionNode : public EnableMakeForLQPNode<ProjectionNode>, public AbstractLQPNode {
 public:
  static ProjectionNodeSPtr make_pass_through(const AbstractLQPNodeSPtr& child);

  explicit ProjectionNode(const std::vector<LQPExpressionSPtr>& column_expressions);

  const std::vector<LQPExpressionSPtr>& column_expressions() const;

  std::string description() const override;

  const std::vector<LQPColumnReference>& output_column_references() const override;
  const std::vector<std::string>& output_column_names() const override;

  std::string get_verbose_column_name(ColumnID column_id) const override;

  bool shallow_equals(const AbstractLQPNode& rhs) const override;

 protected:
  AbstractLQPNodeSPtr _deep_copy_impl(
      const AbstractLQPNodeSPtr& copied_left_input,
      const AbstractLQPNodeSPtr& copied_right_input) const override;
  void _on_input_changed() override;

 private:
  std::vector<LQPExpressionSPtr> _column_expressions;

  mutable std::optional<std::vector<std::string>> _output_column_names;

  void _update_output() const;
};



}  // namespace opossum
