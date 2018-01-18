#pragma once

#include <memory>
#include <unordered_map>

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

class SemanticLQPCompare final {
 public:
  SemanticLQPCompare(const std::shared_ptr<const AbstractLQPNode>& lhs, const std::shared_ptr<const AbstractLQPNode>& rhs);

  bool operator()();

 private:
  std::shared_ptr<const AbstractLQPNode> _lhs;
  std::shared_ptr<const AbstractLQPNode> _rhs;

  std::unordered_map<std::shared_ptr<const AbstractLQPNode>, std::shared_ptr<const AbstractLQPNode>> _node_mapping;

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
};

bool lqp_node_semantic_compare(const std::shared_ptr<const AbstractLQPNode>& lhs, const std::shared_ptr<const AbstractLQPNode>& rhs);

bool lqp_node_types_equal(const std::shared_ptr<const AbstractLQPNode>& lhs, const std::shared_ptr<const AbstractLQPNode>& rhs);

}  // namespace opossum