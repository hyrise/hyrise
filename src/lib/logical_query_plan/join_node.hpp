#pragma once

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "types.hpp"

#include "abstract_lqp_node.hpp"
#include "lqp_column_reference.hpp"

namespace opossum {

using LQPColumnReferencePair = std::pair<LQPColumnReference, LQPColumnReference>;

/**
 * This node type is used to represent any type of Join, including cross products.
 * The idea is that the optimizer is able to decide on the physical join implementation.
 */
class JoinNode : public EnableMakeForLQPNode<JoinNode>, public AbstractLQPNode {
 public:
  // Constructor for Natural and Cross Joins
  explicit JoinNode(const JoinMode join_mode);

  // Constructor for predicated Joins
  JoinNode(const JoinMode join_mode, const LQPColumnReferencePair& join_column_references,
           const PredicateCondition predicate_condition);

  const std::optional<LQPColumnReferencePair>& join_column_references() const;
  const std::optional<PredicateCondition>& predicate_condition() const;
  JoinMode join_mode() const;

  std::string description() const override;
  const std::vector<std::string>& output_column_names() const override;
  const std::vector<LQPColumnReference>& output_column_references() const override;

  std::shared_ptr<TableStatistics> derive_statistics_from(
      const std::shared_ptr<AbstractLQPNode>& left_input,
      const std::shared_ptr<AbstractLQPNode>& right_input) const override;

  std::string get_verbose_column_name(ColumnID column_id) const override;

  bool shallow_equals(const AbstractLQPNode& rhs) const override;

 protected:
  void _on_input_changed() override;
  std::shared_ptr<AbstractLQPNode> _deep_copy_impl(
      const std::shared_ptr<AbstractLQPNode>& copied_left_input,
      const std::shared_ptr<AbstractLQPNode>& copied_right_input) const override;

 private:
  JoinMode _join_mode;
  std::optional<LQPColumnReferencePair> _join_column_references;
  std::optional<PredicateCondition> _predicate_condition;

  mutable std::optional<std::vector<std::string>> _output_column_names;

  void _update_output() const;
};

}  // namespace opossum
