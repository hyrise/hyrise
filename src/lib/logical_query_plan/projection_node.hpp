#pragma once

#include <vector>

#include "abstract_lqp_node.hpp"
#include "expression/abstract_expression.hpp"

namespace opossum {

class ProjectionNode : public EnableMakeForLQPNode<ProjectionNode>, public AbstractLQPNode {
 public:
  explicit ProjectionNode(const std::vector<std::shared_ptr<AbstractExpression>>& expressions);

  std::string description() const override;
  const std::vector<std::shared_ptr<AbstractExpression>>& output_column_expressions() const override;

  std::vector<std::shared_ptr<AbstractExpression>> expressions;

 protected:
  std::shared_ptr<AbstractLQPNode> _shallow_copy_impl(LQPNodeMapping & node_mapping) const override;
  bool _shallow_equals_impl(const AbstractLQPNode& rhs, const LQPNodeMapping & node_mapping) const override;

};

}  // namespace opossum

//#pragma once
//
//#include <memory>
//#include <optional>
//#include <string>
//#include <vector>
//
//#include "abstract_lqp_node.hpp"
//#include "plan_column_definition.hpp"
//
//namespace opossum {
//
//class LQPExpression;
//struct ColumnID;
//
///**
// * Node type to represent common projections, i.e. without any aggregate functionality.
// * It is, however, responsible to calculate arithmetic expressions.
// */
//class ProjectionNode : public EnableMakeForLQPNode<ProjectionNode>, public AbstractLQPNode {
// public:
//  static std::shared_ptr<ProjectionNode> make_pass_through(const std::shared_ptr<AbstractLQPNode>& child);
//
//  explicit ProjectionNode(const std::vector<PlanColumnDefinition>& column_definitions);
//
//  const std::vector<PlanColumnDefinition>& column_definitions() const;
//
//  std::string description() const override;
//
//  const std::vector<LQPColumnReference>& output_column_references() const override;
//  const std::vector<std::string>& output_column_names() const override;
//  const std::vector<std::shared_ptr<AbstractExpression>>& output_column_expressions() const override;
//
//  std::string get_verbose_column_name(ColumnID column_id) const override;
//
//  bool shallow_equals(const AbstractLQPNode& rhs) const override;
//
// protected:
//  std::shared_ptr<AbstractLQPNode> _deep_copy_impl(
//      const std::shared_ptr<AbstractLQPNode>& copied_left_input,
//      const std::shared_ptr<AbstractLQPNode>& copied_right_input) const override;
//  void _on_input_changed() override;
//
// private:
//  const std::vector<PlanColumnDefinition> _column_definitions;
//
//  mutable std::optional<std::vector<std::string>> _output_column_names;
//
//  void _update_output() const;
//};
//
//}  // namespace opossum
