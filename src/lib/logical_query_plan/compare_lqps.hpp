#pragma once

#include <memory>
#include <unordered_map>
#include <vector>

#include "all_parameter_variant.hpp"

namespace opossum {

class AbstractLQPNode;
class AggregateNode;
class CreateViewNode;
class DeleteNode;
class DropViewNode;
class DummyTableNode;
class InsertNode;
class JoinNode;
class LimitNode;
class PredicateNode;
class ProjectionNode;
class LogicalPlanRootNode;
class ShowColumnsNode;
class ShowTablesNode;
class SortNode;
class StoredTableNode;
class UpdateNode;
class ValidateNode;
class MockNode;
class UnionNode;
class LQPColumnReference;
class LQPExpression;

class SemanticLQPCompare final {
 public:
  SemanticLQPCompare(const std::shared_ptr<const AbstractLQPNode>& lhs, const std::shared_ptr<const AbstractLQPNode>& rhs);

  const std::vector<std::pair<std::shared_ptr<const AbstractLQPNode>, std::shared_ptr<const AbstractLQPNode>>>& differing_subtrees() const;

  bool operator()();

 private:
  std::shared_ptr<const AbstractLQPNode> _lhs;
  std::shared_ptr<const AbstractLQPNode> _rhs;

  std::unordered_map<std::shared_ptr<const AbstractLQPNode>, std::shared_ptr<const AbstractLQPNode>> _node_mapping;

  std::vector<std::pair<std::shared_ptr<const AbstractLQPNode>, std::shared_ptr<const AbstractLQPNode>>> _differing_subtrees;

  bool _structural_traverse(const std::shared_ptr<const AbstractLQPNode>& lhs, const std::shared_ptr<const AbstractLQPNode>& rhs);
  bool _semantical_traverse(const std::shared_ptr<const AbstractLQPNode>& lhs, const std::shared_ptr<const AbstractLQPNode>& rhs);
  bool _are_semantically_equal(const std::shared_ptr<const AggregateNode>& lhs, const std::shared_ptr<const AggregateNode>& rhs);
  bool _are_semantically_equal(const std::shared_ptr<const CreateViewNode>& lhs, const std::shared_ptr<const CreateViewNode>& rhs);
  bool _are_semantically_equal(const std::shared_ptr<const DeleteNode>& lhs, const std::shared_ptr<const DeleteNode>& rhs);
  bool _are_semantically_equal(const std::shared_ptr<const DropViewNode>& lhs, const std::shared_ptr<const DropViewNode>& rhs);
  bool _are_semantically_equal(const std::shared_ptr<const DummyTableNode>& lhs, const std::shared_ptr<const DummyTableNode>& rhs);
  bool _are_semantically_equal(const std::shared_ptr<const InsertNode>& lhs, const std::shared_ptr<const InsertNode>& rhs);
  bool _are_semantically_equal(const std::shared_ptr<const JoinNode>& lhs, const std::shared_ptr<const JoinNode>& rhs);
  bool _are_semantically_equal(const std::shared_ptr<const LimitNode>& lhs, const std::shared_ptr<const LimitNode>& rhs);
  bool _are_semantically_equal(const std::shared_ptr<const PredicateNode>& lhs, const std::shared_ptr<const PredicateNode>& rhs);
  bool _are_semantically_equal(const std::shared_ptr<const ProjectionNode>& lhs, const std::shared_ptr<const ProjectionNode>& rhs);
  bool _are_semantically_equal(const std::shared_ptr<const LogicalPlanRootNode>& lhs, const std::shared_ptr<const LogicalPlanRootNode>& rhs);
  bool _are_semantically_equal(const std::shared_ptr<const ShowColumnsNode>& lhs, const std::shared_ptr<const ShowColumnsNode>& rhs);
  bool _are_semantically_equal(const std::shared_ptr<const ShowTablesNode>& lhs, const std::shared_ptr<const ShowTablesNode>& rhs);
  bool _are_semantically_equal(const std::shared_ptr<const SortNode>& lhs, const std::shared_ptr<const SortNode>& rhs);
  bool _are_semantically_equal(const std::shared_ptr<const StoredTableNode>& lhs, const std::shared_ptr<const StoredTableNode>& rhs);
  bool _are_semantically_equal(const std::shared_ptr<const UpdateNode>& lhs, const std::shared_ptr<const UpdateNode>& rhs);
  bool _are_semantically_equal(const std::shared_ptr<const UnionNode>& lhs, const std::shared_ptr<const UnionNode>& rhs);
  bool _are_semantically_equal(const std::shared_ptr<const ValidateNode>& lhs, const std::shared_ptr<const ValidateNode>& rhs);
  bool _are_semantically_equal(const std::shared_ptr<const MockNode>& lhs, const std::shared_ptr<const MockNode>& rhs);

  bool _compare(const std::shared_ptr<const AbstractLQPNode> &lqp_left,
                const std::vector<std::shared_ptr<LQPExpression>> &expressions_left,
                const std::shared_ptr<const AbstractLQPNode> &lqp_right,
                const std::vector<std::shared_ptr<LQPExpression>> &expressions_right) const;
  bool _compare(const std::shared_ptr<const AbstractLQPNode> &lqp_left,
                const std::shared_ptr<const LQPExpression> &expression_left,
                const std::shared_ptr<const AbstractLQPNode> &lqp_right,
                const std::shared_ptr<const LQPExpression> &expression_right) const;

  bool _compare(const std::shared_ptr<const AbstractLQPNode> &lqp_left,
                const std::vector<LQPColumnReference> &column_references_left,
                const std::shared_ptr<const AbstractLQPNode> &lqp_right,
                const std::vector<LQPColumnReference> &column_references_right) const;
  bool _compare(const std::shared_ptr<const AbstractLQPNode> &lqp_left, const LQPColumnReference &column_reference_left,
                const std::shared_ptr<const AbstractLQPNode> &lqp_right,
                const LQPColumnReference &column_reference_right) const;

  bool _compare(const AllParameterVariant& lhs, const AllParameterVariant& rhs) const;
  bool _compare(const AllTypeVariant& lhs, const AllTypeVariant& rhs) const;
};

bool lqp_node_semantic_compare(const std::shared_ptr<const AbstractLQPNode>& lhs, const std::shared_ptr<const AbstractLQPNode>& rhs);

bool lqp_node_types_equal(const std::shared_ptr<const AbstractLQPNode>& lhs, const std::shared_ptr<const AbstractLQPNode>& rhs);

}  // namespace opossum