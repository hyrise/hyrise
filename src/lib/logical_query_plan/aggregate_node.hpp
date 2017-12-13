#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "abstract_lqp_node.hpp"
#include "column_origin.hpp"
#include "types.hpp"

namespace opossum {

class Expression;

/**
 * This node type is used to describe SELECT lists for statements that have at least one of the following:
 *  - one or more aggregate functions in their SELECT list
 *  - a GROUP BY clause
 *
 *  The order of the output columns is groupby columns followed by aggregate columns
 */
class AggregateNode : public AbstractLQPNode {
 public:
  explicit AggregateNode(const std::vector<std::shared_ptr<Expression>>& aggregates,
                         const std::vector<ColumnOrigin>& groupy_column_origins);

  const std::vector<std::shared_ptr<Expression>>& aggregate_expressions() const;
  const std::vector<ColumnOrigin>& groupby_column_origins() const;

  std::string description() const override;

  const std::vector<std::string>& output_column_names() const override;
  const std::vector<std::optional<ColumnID>>& output_column_ids_to_input_column_ids() const override;

  // @{
  /**
   * AggregateNode::find_column_id_for_expression() looks for the @param expression in the columns this
   * node outputs, checking by semantic and NOT by Expression object's address. If it can find it, it will be returned,
   * otherwise std::nullopt is returned.
   * AggregateNode::get_column_id_for_expression() is more strict and will fail, if the
   * @param expression cannot be found
   *
   * Since we're using a TableScan added AFTER the actual aggregate to implement HAVING, in a query like
   * `SELECT MAX(a) FROM t1 GROUP BY b HAVING MAX(a) > 10`
   * we need get the column that contains the `MAX(a)` in the table produced by the Aggregate. This is what this
   * function is used for.
   *
   * NOTE: These functions will possibly result in a full recursive traversal of the ancestors of this node.
   */
  std::optional<ColumnID> find_column_id_for_expression(const std::shared_ptr<Expression>& expression) const;
  ColumnID get_column_id_for_expression(const std::shared_ptr<Expression>& expression) const;
  // @}

  std::vector<ColumnID> get_output_column_ids_for_table(const std::string& table_name) const override;

  std::string get_verbose_column_name(ColumnID column_id) const override;

 protected:
  void _on_child_changed() override;

 private:
  std::vector<std::shared_ptr<Expression>> _aggregate_expressions;
  std::vector<ColumnOrigin> _groupby_column_origins;

  mutable std::optional<std::vector<std::string>> _output_column_names;

  void _update_output() const;
};

}  // namespace opossum
