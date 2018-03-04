#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "abstract_lqp_node.hpp"
#include "lqp_column_reference.hpp"
#include "types.hpp"

namespace opossum {

// TODO(anybody) Use instead of LQPExpression once the AggregateNode doesn't need to contain expressions as function
// args anymore and the SQLTranslator creates a ProjectionNode instead
// using AggregateNodeColumnDefinition = AggregateColumnDefinitionTemplate<LQPColumnReference>;

/**
 * This node type is used to describe SELECT lists for statements that have at least one of the following:
 *  - one or more aggregate functions in their SELECT list
 *  - a GROUP BY clause
 *
 *  The order of the output columns is groupby columns followed by aggregate columns
 */
class AggregateNode : public EnableMakeForLQPNode<AggregateNode>, public AbstractLQPNode {
 public:
  explicit AggregateNode(const std::vector<std::shared_ptr<LQPExpression>>& aggregates,
                         const std::vector<LQPColumnReference>& groupby_column_references);

  const std::vector<std::shared_ptr<LQPExpression>>& aggregate_expressions() const;
  const std::vector<LQPColumnReference>& groupby_column_references() const;

  std::string description() const override;

  const std::vector<std::string>& output_column_names() const override;
  const std::vector<LQPColumnReference>& output_column_references() const override;

  // @{
  /**
   * AggregateNode::find_column_by_expression() looks for the @param expression in the columns this
   * node outputs, checking by semantic and NOT by Expression object's address. If it can find it, it will be returned,
   * otherwise std::nullopt is returned.
   * AggregateNode::get_column_by_expression() is more strict and will fail, if the
   * @param expression cannot be found
   *
   * Since we're using a TableScan added AFTER the actual aggregate to implement HAVING, in a query like
   * `SELECT MAX(a) FROM t1 GROUP BY b HAVING MAX(a) > 10`
   * we need get the column that contains the `MAX(a)` in the table produced by the Aggregate. This is what this
   * function is used for.
   *
   * NOTE: These functions will possibly result in a full recursive traversal of the ancestors of this node.
   */
  std::optional<LQPColumnReference> find_column_by_expression(const std::shared_ptr<LQPExpression>& expression) const;
  LQPColumnReference get_column_by_expression(const std::shared_ptr<LQPExpression>& expression) const;
  // @}

  std::string get_verbose_column_name(ColumnID column_id) const override;

  bool shallow_equals(const AbstractLQPNode& rhs) const override;

 protected:
  std::shared_ptr<AbstractLQPNode> _deep_copy_impl(
      const std::shared_ptr<AbstractLQPNode>& copied_left_input,
      const std::shared_ptr<AbstractLQPNode>& copied_right_input) const override;
  void _on_input_changed() override;

 private:
  std::vector<std::shared_ptr<LQPExpression>> _aggregate_expressions;
  std::vector<LQPColumnReference> _groupby_column_references;

  mutable std::optional<std::vector<std::string>> _output_column_names;

  void _update_output() const;
};

}  // namespace opossum
