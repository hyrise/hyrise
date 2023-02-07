#include "union_node.hpp"

#include <memory>
#include <numeric>
#include <string>
#include <vector>

#include "constant_mappings.hpp"
#include "expression/expression_utils.hpp"
#include "utils/assert.hpp"

namespace hyrise {

UnionNode::UnionNode(const SetOperationMode init_set_operation_mode)
    : AbstractLQPNode(LQPNodeType::Union), set_operation_mode(init_set_operation_mode) {}

std::string UnionNode::description(const DescriptionMode mode) const {
  return "[UnionNode] Mode: " + set_operation_mode_to_string.left.at(set_operation_mode);
}

std::vector<std::shared_ptr<AbstractExpression>> UnionNode::output_expressions() const {
  const auto& left_expressions = left_input()->output_expressions();
  /**
   * Asserting matching table schemas leads to multiple fetches of a subplan's output expressions. Though this does not
   * have performance implications now, they may arise in the future. In this case, consider relaxing the check by using
   * `DebugAssert`.
   */
  Assert(expressions_equal(left_expressions, right_input()->output_expressions()), "Input Expressions must match");
  return left_expressions;
}

bool UnionNode::is_column_nullable(const ColumnID column_id) const {
  Assert(left_input() && right_input(), "Need both inputs to determine nullability");

  return left_input()->is_column_nullable(column_id) || right_input()->is_column_nullable(column_id);
}

UniqueColumnCombinations UnionNode::unique_column_combinations() const {
  switch (set_operation_mode) {
    case SetOperationMode::Positions: {
      /**
       * UnionPositions merges two reference tables with the same original table(s), computing a Set Union of their
       * PosLists. Thus, unique column combinations remain valid.
       *
       * In the cases that we have seen so far (usually disjunctions turned into individual nodes by the
       * PredicateSplitUpRule), the UCCs have remained the same on the left and right sides. This is not necessarily
       * true in the future. We assert this behaviour so that we are aware if this ever changes.
       */
      const auto& left_unique_column_combinations = _forward_left_unique_column_combinations();
      Assert(left_unique_column_combinations == right_input()->unique_column_combinations(),
             "Input tables should have the same unique column combinations.");
      return left_unique_column_combinations;
    }
    case SetOperationMode::All: {
      /**
       * With UnionAll, two tables become merged. The resulting table might contain duplicates. To forward UCCs from
       * child nodes, we would have to ensure that both input tables are completely distinct in terms of rows.
       * Currently, there is no strategy. Thus, we discard all unique column combinations.
       */
      return UniqueColumnCombinations{};
    }
    case SetOperationMode::Unique:
      Fail("ToDo, see discussion https://github.com/hyrise/hyrise/pull/2156#discussion_r452803825");
  }
  Fail("Unhandled UnionMode");
}

OrderDependencies UnionNode::order_dependencies() const {
  switch (set_operation_mode) {
    case SetOperationMode::Positions: {
      const auto& left_order_dependencies = left_input()->order_dependencies();
      Assert(left_order_dependencies == right_input()->order_dependencies(),
             "Input tables should have the same constraints.");
      return left_order_dependencies;
    }
    case SetOperationMode::All: {
      /**
       * With UnionAll, two tables become merged. The resulting table might contain duplicates. To forward ODs from
       * child nodes, we would have to ensure that both input tables are completely distinct in terms of rows.
       * Currently, there is no strategy. Thus, we discard all unique column combinations.
       */
      return OrderDependencies{};
    }
    case SetOperationMode::Unique:
      Fail("ToDo, see discussion https://github.com/hyrise/hyrise/pull/2156#discussion_r452803825");
  }
  Fail("Unhandled UnionMode");
}

std::shared_ptr<InclusionDependencies> UnionNode::inclusion_dependencies() const {
  switch (set_operation_mode) {
    case SetOperationMode::Positions: {
      const auto& left_inclusion_dependencies = left_input()->inclusion_dependencies();
      Assert(*left_inclusion_dependencies == *right_input()->inclusion_dependencies(),
             "Input tables should have the same constraints.");
      return left_inclusion_dependencies;
    }
    case SetOperationMode::All: {
      const auto& left_inclusion_dependencies = left_input()->inclusion_dependencies();
      const auto& right_inclusion_dependencies = right_input()->inclusion_dependencies();
      const auto inclusion_dependencies = std::make_shared<InclusionDependencies>(left_inclusion_dependencies->cbegin(),
                                                                                  left_inclusion_dependencies->cend());
      inclusion_dependencies->insert(right_inclusion_dependencies->cbegin(), right_inclusion_dependencies->cend());
      return inclusion_dependencies;
    }
    case SetOperationMode::Unique:
      Fail("ToDo, see discussion https://github.com/hyrise/hyrise/pull/2156#discussion_r452803825");
  }
  Fail("Unhandled UnionMode");
}

FunctionalDependencies UnionNode::non_trivial_functional_dependencies() const {
  switch (set_operation_mode) {
    case SetOperationMode::All: {
      /**
       * With UnionAll, UCCs from both input nodes are discarded. To preserve trivial FDs, we request all available FDs
       * from both input nodes.
       */
      const auto& fds_left = left_input()->functional_dependencies();
      const auto& fds_right = right_input()->functional_dependencies();
      /**
       * Currently, both input tables have the same output expressions for SetOperationMode::All. However, the FDs might
       * differ. For example, the left input node could have discarded FDs, whereas the right one has not. To work
       * around this issue, we return the intersected set of FDs which is valid for both input nodes.
       */
      return intersect_fds(fds_left, fds_right);
    }
    case SetOperationMode::Positions: {
      /**
       * By definition, UnionPositions requires both input tables to have the same table origin and structure.
       * Therefore, we can pass the FDs of either the left or the right input node.
       */
      const auto& non_trivial_fds = left_input()->non_trivial_functional_dependencies();
      DebugAssert(non_trivial_fds == right_input()->non_trivial_functional_dependencies(),
                  "Expected both input nodes to pass the same non-trivial FDs.");
      return non_trivial_fds;
    }
    default: {
      Fail("Unhandled UnionMode");
    }
  }
}

size_t UnionNode::_on_shallow_hash() const {
  return boost::hash_value(set_operation_mode);
}

std::shared_ptr<AbstractLQPNode> UnionNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return UnionNode::make(set_operation_mode);
}

bool UnionNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& union_node = static_cast<const UnionNode&>(rhs);
  return set_operation_mode == union_node.set_operation_mode;
}

}  // namespace hyrise
