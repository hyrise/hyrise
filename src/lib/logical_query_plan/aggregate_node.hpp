#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "abstract_lqp_node.hpp"
#include "lqp_column_reference.hpp"
#include "named_expression.hpp"
#include "types.hpp"

namespace opossum {

/**
 * This node type is used to describe SELECT lists for statements that have at least one of the following:
 *  - one or more aggregate functions in their SELECT list
 *  - a GROUP BY clause
 *
 *  The order of the output columns is groupby columns followed by aggregate columns
 */
class AggregateNode : public EnableMakeForLQPNode<AggregateNode>, public AbstractLQPNode {
 public:
  AggregateNode(const std::vector<LQPColumnReference>& groupby_column_references,
                const std::vector<NamedExpression>& named_aggregate_expressions);

  const std::vector<NamedExpression>& named_aggregate_expressions() const;
  const std::vector<LQPColumnReference>& groupby_column_references() const;

  std::string description() const override;

  const std::vector<std::string>& output_column_names() const override;
  const std::vector<LQPColumnReference>& output_column_references() const override;
  const std::vector<std::shared_ptr<AbstractExpression>>& output_column_expressions() const override;

  std::string get_verbose_column_name(ColumnID column_id) const override;

  bool shallow_equals(const AbstractLQPNode& rhs) const override;

 protected:
  std::shared_ptr<AbstractLQPNode> _deep_copy_impl(
      const std::shared_ptr<AbstractLQPNode>& copied_left_input,
      const std::shared_ptr<AbstractLQPNode>& copied_right_input) const override;
  void _on_input_changed() override;

 private:
  std::vector<NamedExpression> _named_aggregate_expressions;
  std::vector<LQPColumnReference> _groupby_column_references;

  mutable std::optional<std::vector<std::string>> _output_column_names;

  void _update_output() const;
};

}  // namespace opossum
