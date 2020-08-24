#include "union_node.hpp"

#include <memory>
#include <numeric>
#include <string>
#include <vector>

#include "constant_mappings.hpp"
#include "expression/expression_utils.hpp"
#include "utils/assert.hpp"

namespace opossum {

UnionNode::UnionNode(const SetOperationMode init_set_operation_mode)
    : AbstractLQPNode(LQPNodeType::Union), set_operation_mode(init_set_operation_mode) {}

std::string UnionNode::description(const DescriptionMode mode) const {
  return "[UnionNode] Mode: " + set_operation_mode_to_string.left.at(set_operation_mode);
}

std::vector<std::shared_ptr<AbstractExpression>> UnionNode::output_expressions() const {
  Assert(expressions_equal(left_input()->output_expressions(), right_input()->output_expressions()),
         "Input Expressions must match");
  return left_input()->output_expressions();
}

bool UnionNode::is_column_nullable(const ColumnID column_id) const {
  Assert(left_input() && right_input(), "Need both inputs to determine nullability");

  return left_input()->is_column_nullable(column_id) || right_input()->is_column_nullable(column_id);
}

std::shared_ptr<LQPUniqueConstraints> UnionNode::unique_constraints() const {
  switch (set_operation_mode) {
    case SetOperationMode::Positions: {
      /**
       * UnionPositions merges two reference tables with the same original table(s), computing a Set Union of their
       * PosLists. Therefore, unique constraints remain valid.
       *
       * In the cases that we have seen so far (usually disjunctions turned into individual nodes by the
       * PredicateSplitUpRule), the constraints have remained the same on the left and right sides. This is not
       * necessarily true in the future. We assert this behaviour so that we are aware if this ever changes.
       */
      const auto& left_unique_constraints = _forward_left_unique_constraints();
      Assert(*left_unique_constraints == *right_input()->unique_constraints(),
             "Input tables should have the same constraints.");
      return left_unique_constraints;
    }
    case SetOperationMode::All: {
      /**
       * With UnionAll, two tables become merged. The resulting table might contain duplicates.
       * To forward constraints from child nodes, we would have to ensure that both input tables are completely
       * distinct in terms of rows. Currently, there is no strategy. Therefore, we discard all unique constraints.
       */
      return std::make_shared<LQPUniqueConstraints>();
    }
    case SetOperationMode::Unique:
      Fail("ToDo, see discussion https://github.com/hyrise/hyrise/pull/2156#discussion_r452803825");
  }
  Fail("Unhandled UnionMode");
}

std::vector<FunctionalDependency> UnionNode::non_trivial_functional_dependencies() const {
  switch (set_operation_mode) {
    case SetOperationMode::All: {
      /**
       * With UnionAll, unique constraints from both input nodes become discarded. To preserve trivial FDs, we
       * request all available FDs from both input nodes.
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

size_t UnionNode::_on_shallow_hash() const { return boost::hash_value(set_operation_mode); }

std::shared_ptr<AbstractLQPNode> UnionNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return UnionNode::make(set_operation_mode);
}

bool UnionNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& union_node = static_cast<const UnionNode&>(rhs);
  return set_operation_mode == union_node.set_operation_mode;
}

}  // namespace opossum
