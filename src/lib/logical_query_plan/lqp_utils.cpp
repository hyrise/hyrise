#include "lqp_utils.hpp"

#include "expression/abstract_expression.hpp"
#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "expression/lqp_subquery_expression.hpp"
#include "logical_query_plan/change_meta_table_node.hpp"
#include "logical_query_plan/insert_node.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/static_table_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "logical_query_plan/update_node.hpp"
#include "utils/assert.hpp"

using namespace hyrise::expression_functional;  // NOLINT

namespace {

using namespace hyrise;  // NOLINT

void lqp_create_node_mapping_impl(LQPNodeMapping& mapping, const std::shared_ptr<AbstractLQPNode>& lhs,
                                  const std::shared_ptr<AbstractLQPNode>& rhs) {
  if (!lhs && !rhs) {
    return;
  }

  Assert(lhs && rhs, "LQPs aren't equally structured, can't create mapping");
  Assert(lhs->type == rhs->type, "LQPs aren't equally structured, can't create mapping");

  // To avoid traversing subgraphs of ORs twice, check whether we've been here already
  const auto mapping_iter = mapping.find(lhs);
  if (mapping_iter != mapping.end()) {
    return;
  }

  mapping[lhs] = rhs;

  lqp_create_node_mapping_impl(mapping, lhs->left_input(), rhs->left_input());
  lqp_create_node_mapping_impl(mapping, lhs->right_input(), rhs->right_input());
}

std::optional<LQPMismatch> lqp_find_structure_mismatch(const std::shared_ptr<const AbstractLQPNode>& lhs,
                                                       const std::shared_ptr<const AbstractLQPNode>& rhs) {
  if (!lhs && !rhs) {
    return std::nullopt;
  }

  if (!(lhs && rhs) || lhs->type != rhs->type) {
    return LQPMismatch(lhs, rhs);
  }

  auto mismatch_left = lqp_find_structure_mismatch(lhs->left_input(), rhs->left_input());
  if (mismatch_left) {
    return mismatch_left;
  }

  return lqp_find_structure_mismatch(lhs->right_input(), rhs->right_input());
}

std::optional<LQPMismatch> lqp_find_subplan_mismatch_impl(const LQPNodeMapping& node_mapping,
                                                          const std::shared_ptr<const AbstractLQPNode>& lhs,
                                                          const std::shared_ptr<const AbstractLQPNode>& rhs) {
  if (!lhs && !rhs) {
    return std::nullopt;
  }

  if (!lhs->shallow_equals(*rhs, node_mapping)) {
    return LQPMismatch(lhs, rhs);
  }

  auto mismatch_left = lqp_find_subplan_mismatch_impl(node_mapping, lhs->left_input(), rhs->left_input());
  if (mismatch_left) {
    return mismatch_left;
  }

  return lqp_find_subplan_mismatch_impl(node_mapping, lhs->right_input(), rhs->right_input());
}

void lqp_find_subplan_roots_impl(std::vector<std::shared_ptr<AbstractLQPNode>>& root_nodes,
                                 std::unordered_set<std::shared_ptr<AbstractLQPNode>>& visited_nodes,
                                 const std::shared_ptr<AbstractLQPNode>& lqp) {
  root_nodes.emplace_back(lqp);

  visit_lqp(lqp, [&](const auto& sub_node) {
    if (!visited_nodes.emplace(sub_node).second) {
      return LQPVisitation::DoNotVisitInputs;
    }

    for (const auto& expression : sub_node->node_expressions) {
      visit_expression(expression, [&](const auto sub_expression) {
        if (const auto subquery_expression = std::dynamic_pointer_cast<LQPSubqueryExpression>(sub_expression)) {
          lqp_find_subplan_roots_impl(root_nodes, visited_nodes, subquery_expression->lqp);
        }

        return ExpressionVisitation::VisitArguments;
      });
    }

    return LQPVisitation::VisitInputs;
  });
}

void recursively_collect_lqp_subquery_expressions_by_lqp(
    SubqueryExpressionsByLQP& subquery_expressions_by_lqp, const std::shared_ptr<AbstractLQPNode>& node,
    std::unordered_set<std::shared_ptr<AbstractLQPNode>>& visited_nodes) {
  if (!node || !visited_nodes.emplace(node).second) {
    return;
  }

  for (const auto& expression : node->node_expressions) {
    visit_expression(expression, [&](const auto& sub_expression) {
      const auto subquery_expression = std::dynamic_pointer_cast<LQPSubqueryExpression>(sub_expression);
      if (!subquery_expression) {
        return ExpressionVisitation::VisitArguments;
      }

      for (auto& [lqp, subquery_expressions] : subquery_expressions_by_lqp) {
        if (*lqp == *subquery_expression->lqp) {
          subquery_expressions.emplace_back(subquery_expression);
          return ExpressionVisitation::DoNotVisitArguments;
        }
      }
      subquery_expressions_by_lqp.emplace(subquery_expression->lqp,
                                          std::vector{std::weak_ptr<LQPSubqueryExpression>(subquery_expression)});

      // Subqueries can be nested. We are also interested in the LQPs from deeply nested subqueries.
      recursively_collect_lqp_subquery_expressions_by_lqp(subquery_expressions_by_lqp, subquery_expression->lqp,
                                                          visited_nodes);

      return ExpressionVisitation::DoNotVisitArguments;
    });
  }

  recursively_collect_lqp_subquery_expressions_by_lqp(subquery_expressions_by_lqp, node->left_input(), visited_nodes);
  recursively_collect_lqp_subquery_expressions_by_lqp(subquery_expressions_by_lqp, node->right_input(), visited_nodes);
}

}  // namespace

namespace hyrise {

SubqueryExpressionsByLQP collect_lqp_subquery_expressions_by_lqp(const std::shared_ptr<AbstractLQPNode>& node) {
  auto visited_nodes = std::unordered_set<std::shared_ptr<AbstractLQPNode>>();
  auto subqueries_by_lqp = SubqueryExpressionsByLQP{};
  recursively_collect_lqp_subquery_expressions_by_lqp(subqueries_by_lqp, node, visited_nodes);

  return subqueries_by_lqp;
}

LQPNodeMapping lqp_create_node_mapping(const std::shared_ptr<AbstractLQPNode>& lhs,
                                       const std::shared_ptr<AbstractLQPNode>& rhs) {
  LQPNodeMapping mapping;
  lqp_create_node_mapping_impl(mapping, lhs, rhs);
  return mapping;
}

std::optional<LQPMismatch> lqp_find_subplan_mismatch(const std::shared_ptr<const AbstractLQPNode>& lhs,
                                                     const std::shared_ptr<const AbstractLQPNode>& rhs) {
  // Check for type/structural mismatched
  auto mismatch = lqp_find_structure_mismatch(lhs, rhs);
  if (mismatch) {
    return mismatch;
  }

  // For lqp_create_node_mapping() we need mutable pointers - but won't use them to manipulate, promised.
  // It's just that NodeMapping has takes a mutable ptr in as the value type
  const auto mutable_lhs = std::const_pointer_cast<AbstractLQPNode>(lhs);
  const auto mutable_rhs = std::const_pointer_cast<AbstractLQPNode>(rhs);
  const auto node_mapping = lqp_create_node_mapping(mutable_lhs, mutable_rhs);

  return lqp_find_subplan_mismatch_impl(node_mapping, lhs, rhs);
}

void lqp_replace_node(const std::shared_ptr<AbstractLQPNode>& original_node,
                      const std::shared_ptr<AbstractLQPNode>& replacement_node) {
  DebugAssert(replacement_node->outputs().empty(), "Node can't have outputs");
  DebugAssert(!replacement_node->left_input() && !replacement_node->right_input(),
              "Replacement node can't have inputs");

  const auto outputs = original_node->outputs();
  const auto input_sides = original_node->get_input_sides();

  /**
   * Tie the replacement_node with this nodes inputs
   */
  replacement_node->set_left_input(original_node->left_input());
  replacement_node->set_right_input(original_node->right_input());

  /**
   * Tie the replacement_node with this nodes outputs.
   */
  const auto output_count = outputs.size();
  for (auto output_idx = size_t{0}; output_idx < output_count; ++output_idx) {
    outputs[output_idx]->set_input(input_sides[output_idx], replacement_node);
  }

  /**
   * Untie this node from the LQP
   */
  original_node->set_left_input(nullptr);
  original_node->set_right_input(nullptr);
}

void lqp_remove_node(const std::shared_ptr<AbstractLQPNode>& node, const AllowRightInput allow_right_input) {
  Assert(allow_right_input == AllowRightInput::Yes || !node->right_input(),
         "Caller did not explicitly confirm that right input should be ignored");

  /**
   * Back up outputs and in which input side they hold this node
   */
  auto outputs = node->outputs();
  auto input_sides = node->get_input_sides();

  /**
   * Hold left_input ptr in extra variable to keep the ref count up and untie it from this node.
   * left_input might be nullptr
   */
  auto left_input = node->left_input();
  node->set_left_input(nullptr);

  /**
   * Tie this node's previous outputs with this nodes previous left input
   * If left_input is nullptr, still call set_input so this node will get untied from the LQP.
   */
  const auto output_count = outputs.size();
  for (auto output_idx = size_t{0}; output_idx < output_count; ++output_idx) {
    outputs[output_idx]->set_input(input_sides[output_idx], left_input);
  }
}

void lqp_insert_node(const std::shared_ptr<AbstractLQPNode>& parent_node, const LQPInputSide input_side,
                     const std::shared_ptr<AbstractLQPNode>& node_to_insert, const AllowRightInput allow_right_input) {
  DebugAssert(!node_to_insert->left_input(), "Expected node without a left input.");
  DebugAssert(!node_to_insert->right_input() || allow_right_input == AllowRightInput::Yes,
              "Expected node without a right input.");
  DebugAssert(node_to_insert->output_count() == 0, "Expected node without outputs.");

  const auto old_input = parent_node->input(input_side);
  parent_node->set_input(input_side, node_to_insert);
  node_to_insert->set_left_input(old_input);
}

void lqp_insert_node_above(const std::shared_ptr<AbstractLQPNode>& node,
                           const std::shared_ptr<AbstractLQPNode>& node_to_insert,
                           const AllowRightInput allow_right_input) {
  DebugAssert(!node_to_insert->left_input(), "Expected node without a left input.");
  DebugAssert(!node_to_insert->right_input() || allow_right_input == AllowRightInput::Yes,
              "Expected node without a right input.");
  DebugAssert(node_to_insert->output_count() == 0, "Expected node without outputs.");

  // Re-link @param node's outputs to @param node_to_insert
  const auto node_outputs = node->outputs();
  for (const auto& output_node : node_outputs) {
    const LQPInputSide input_side = node->get_input_side(output_node);
    output_node->set_input(input_side, node_to_insert);
  }

  // Place @param node_to_insert above @param node
  node_to_insert->set_left_input(node);
}

bool lqp_is_validated(const std::shared_ptr<AbstractLQPNode>& lqp) {
  if (!lqp || lqp->type == LQPNodeType::Validate) {
    return true;
  }

  if (!lqp->left_input() && !lqp->right_input()) {
    return false;
  }

  return lqp_is_validated(lqp->left_input()) && lqp_is_validated(lqp->right_input());
}

std::set<std::string> lqp_find_modified_tables(const std::shared_ptr<AbstractLQPNode>& lqp) {
  std::set<std::string> modified_tables;

  visit_lqp(lqp, [&](const auto& node) {
    switch (node->type) {
      case LQPNodeType::Insert:
        modified_tables.insert(static_cast<InsertNode&>(*node).table_name);
        break;
      case LQPNodeType::Update:
        modified_tables.insert(static_cast<UpdateNode&>(*node).table_name);
        break;
      case LQPNodeType::Delete: {
        visit_lqp(node->left_input(), [&](const auto& sub_delete_node) {
          if (const auto stored_table_node = std::dynamic_pointer_cast<StoredTableNode>(sub_delete_node)) {
            modified_tables.insert(stored_table_node->table_name);
          } else if (const auto mock_node = std::dynamic_pointer_cast<MockNode>(sub_delete_node)) {
            if (mock_node->name) {
              modified_tables.insert(*mock_node->name);
            }
          }
          return LQPVisitation::VisitInputs;
        });
      } break;
      case LQPNodeType::ChangeMetaTable:
        modified_tables.insert(static_cast<ChangeMetaTableNode&>(*node).table_name);
        break;
      case LQPNodeType::CreateTable:
      case LQPNodeType::CreatePreparedPlan:
      case LQPNodeType::DropTable:
      case LQPNodeType::Validate:
      case LQPNodeType::Aggregate:
      case LQPNodeType::Alias:
      case LQPNodeType::CreateView:
      case LQPNodeType::DropView:
      case LQPNodeType::DummyTable:
      case LQPNodeType::Export:
      case LQPNodeType::Import:
      case LQPNodeType::Join:
      case LQPNodeType::Limit:
      case LQPNodeType::Predicate:
      case LQPNodeType::Projection:
      case LQPNodeType::Root:
      case LQPNodeType::Sort:
      case LQPNodeType::StaticTable:
      case LQPNodeType::StoredTable:
      case LQPNodeType::Union:
      case LQPNodeType::Intersect:
      case LQPNodeType::Except:
      case LQPNodeType::Mock:
        return LQPVisitation::VisitInputs;
    }
    return LQPVisitation::VisitInputs;
  });

  return modified_tables;
}

namespace {
/**
 * Function creates a boolean expression from an lqp. It traverses the passed lqp from top to bottom. However, an lqp is
 * evaluated from bottom to top. This requires that the order in which the translated expressions are added to the
 * output expression is the reverse order of how the nodes are traversed. The subsequent_expression parameter passes the
 * translated expressions to the translation of its children nodes, which allows to add the translated expression of
 * child node before its parent node to the output expression.
 */
std::shared_ptr<AbstractExpression> lqp_subplan_to_boolean_expression_impl(
    const std::shared_ptr<AbstractLQPNode>& begin, const std::optional<const std::shared_ptr<AbstractLQPNode>>& end,
    const std::optional<const std::shared_ptr<AbstractExpression>>& subsequent_expression) {
  if (end && begin == *end) {
    return nullptr;
  }

  switch (begin->type) {
    case LQPNodeType::Predicate: {
      const auto predicate_node = std::dynamic_pointer_cast<PredicateNode>(begin);
      const auto predicate = predicate_node->predicate();
      auto expression = subsequent_expression ? and_(predicate, *subsequent_expression) : predicate;
      auto left_input_expression = lqp_subplan_to_boolean_expression_impl(begin->left_input(), end, expression);
      if (left_input_expression) {
        return left_input_expression;
      }

      return expression;
    }

    case LQPNodeType::Union: {
      const auto union_node = std::dynamic_pointer_cast<UnionNode>(begin);
      const auto left_input_expression = lqp_subplan_to_boolean_expression_impl(begin->left_input(), end, std::nullopt);
      const auto right_input_expression =
          lqp_subplan_to_boolean_expression_impl(begin->right_input(), end, std::nullopt);
      if (left_input_expression && right_input_expression) {
        const auto or_expression = or_(left_input_expression, right_input_expression);
        return subsequent_expression ? and_(or_expression, *subsequent_expression) : or_expression;
      }

      return nullptr;
    }

    case LQPNodeType::Projection:
    case LQPNodeType::Sort:
    case LQPNodeType::Validate:
    case LQPNodeType::Limit:
      return lqp_subplan_to_boolean_expression_impl(begin->left_input(), end, subsequent_expression);

    default:
      return nullptr;
  }
}
}  // namespace

// Function wraps the call to the lqp_subplan_to_boolean_expression_impl() function to hide its third parameter,
// subsequent_predicate, which is only used internally.
std::shared_ptr<AbstractExpression> lqp_subplan_to_boolean_expression(
    const std::shared_ptr<AbstractLQPNode>& begin, const std::optional<const std::shared_ptr<AbstractLQPNode>>& end) {
  return lqp_subplan_to_boolean_expression_impl(begin, end, std::nullopt);
}

std::vector<std::shared_ptr<AbstractLQPNode>> lqp_find_subplan_roots(const std::shared_ptr<AbstractLQPNode>& lqp) {
  auto root_nodes = std::vector<std::shared_ptr<AbstractLQPNode>>{};
  auto visited_nodes = std::unordered_set<std::shared_ptr<AbstractLQPNode>>{};
  lqp_find_subplan_roots_impl(root_nodes, visited_nodes, lqp);
  return root_nodes;
}

std::vector<std::shared_ptr<AbstractLQPNode>> lqp_find_nodes_by_type(const std::shared_ptr<AbstractLQPNode>& lqp,
                                                                     const LQPNodeType type) {
  std::vector<std::shared_ptr<AbstractLQPNode>> nodes;
  visit_lqp(lqp, [&](const auto& node) {
    if (node->type == type) {
      nodes.emplace_back(node);
    }
    return LQPVisitation::VisitInputs;
  });

  return nodes;
}

std::vector<std::shared_ptr<AbstractLQPNode>> lqp_find_leaves(const std::shared_ptr<AbstractLQPNode>& lqp) {
  std::vector<std::shared_ptr<AbstractLQPNode>> nodes;
  visit_lqp(lqp, [&](const auto& node) {
    if (node->input_count() > 0) {
      return LQPVisitation::VisitInputs;
    }

    nodes.emplace_back(node);
    return LQPVisitation::DoNotVisitInputs;
  });

  return nodes;
}

ExpressionUnorderedSet find_column_expressions(const AbstractLQPNode& lqp_node, const std::set<ColumnID>& column_ids) {
  DebugAssert(lqp_node.type == LQPNodeType::StoredTable || lqp_node.type == LQPNodeType::StaticTable ||
                  lqp_node.type == LQPNodeType::Mock,
              "Did not expect other node types than StoredTableNode, StaticTableNode and MockNode.");
  DebugAssert(!lqp_node.left_input(), "Only valid for data source nodes");

  const auto& output_expressions = lqp_node.output_expressions();
  auto column_expressions = ExpressionUnorderedSet{};
  column_expressions.reserve(column_ids.size());

  for (const auto& output_expression : output_expressions) {
    const auto column_expression = dynamic_pointer_cast<LQPColumnExpression>(output_expression);
    if (column_expression && column_ids.contains(column_expression->original_column_id) &&
        *column_expression->original_node.lock() == lqp_node) {
      [[maybe_unused]] const auto [_, success] = column_expressions.emplace(column_expression);
      DebugAssert(success, "Did not expect multiple column expressions for the same column id.");
    }
  }

  return column_expressions;
}

bool contains_matching_unique_constraint(const std::shared_ptr<LQPUniqueConstraints>& unique_constraints,
                                         const ExpressionUnorderedSet& expressions) {
  DebugAssert(!unique_constraints->empty(), "Invalid input: Set of unique constraints should not be empty.");
  DebugAssert(!expressions.empty(), "Invalid input: Set of expressions should not be empty.");

  // Look for a unique constraint that is based on a subset of the given expressions
  for (const auto& unique_constraint : *unique_constraints) {
    if (unique_constraint.expressions.size() <= expressions.size() &&
        std::all_of(unique_constraint.expressions.cbegin(), unique_constraint.expressions.cend(),
                    [&expressions](const auto unique_constraint_expression) {
                      return expressions.contains(unique_constraint_expression);
                    })) {
      // Found a matching unique constraint
      return true;
    }
  }
  // Did not find a unique constraint for the given expressions
  return false;
}

std::vector<FunctionalDependency> fds_from_unique_constraints(
    const std::shared_ptr<const AbstractLQPNode>& lqp,
    const std::shared_ptr<LQPUniqueConstraints>& unique_constraints) {
  Assert(!unique_constraints->empty(), "Did not expect empty vector of unique constraints");

  auto fds = std::vector<FunctionalDependency>{};

  // Collect non-nullable output expressions
  const auto& output_expressions = lqp->output_expressions();
  auto output_expressions_non_nullable = ExpressionUnorderedSet{};
  for (auto column_id = ColumnID{0}; column_id < output_expressions.size(); ++column_id) {
    if (!lqp->is_column_nullable(column_id)) {
      output_expressions_non_nullable.insert(output_expressions.at(column_id));
    }
  }

  for (const auto& unique_constraint : *unique_constraints) {
    auto determinants = unique_constraint.expressions;

    // (1) Verify whether we can create an FD from the given unique constraint (non-nullable determinant expressions)
    if (!std::all_of(determinants.cbegin(), determinants.cend(),
                     [&output_expressions_non_nullable](const auto& determinant_expression) {
                       return output_expressions_non_nullable.contains(determinant_expression);
                     })) {
      continue;
    }

    // (2) Collect the dependent output expressions
    auto dependents = ExpressionUnorderedSet();
    for (const auto& output_expression : output_expressions) {
      if (determinants.contains(output_expression)) {
        continue;
      }
      dependents.insert(output_expression);
    }

    // (3) Add FD to output
    if (dependents.empty()) {
      continue;
    }
    DebugAssert(std::find_if(fds.cbegin(), fds.cend(),
                             [&determinants, &dependents](const auto& fd) {
                               return (fd.determinants == determinants) && (fd.dependents == dependents);
                             }) == fds.cend(),
                "Creating duplicate functional dependencies is unexpected.");
    fds.emplace_back(determinants, dependents);
  }
  return fds;
}

void remove_invalid_fds(const std::shared_ptr<const AbstractLQPNode>& lqp, std::vector<FunctionalDependency>& fds) {
  if (fds.empty()) {
    return;
  }

  const auto& output_expressions = lqp->output_expressions();
  const auto& output_expressions_set = ExpressionUnorderedSet{output_expressions.cbegin(), output_expressions.cend()};

  // Adjust FDs: Remove dependents that are not part of the node's output expressions
  auto not_part_of_output_expressions = [&output_expressions_set](const auto& fd_dependent_expression) {
    return !output_expressions_set.contains(fd_dependent_expression);
  };

  for (auto& fd : fds) {
    std::erase_if(fd.dependents, not_part_of_output_expressions);
  }

  // Remove invalid or unnecessary FDs
  fds.erase(std::remove_if(fds.begin(), fds.end(),
                           [&](auto& fd) {
                             // If there are no dependents left, we can discard the FD altogether
                             if (fd.dependents.empty()) {
                               return true;
                             }

                             /**
                              * Remove FDs with determinant expressions that are
                              *  a) not part of the node's output expressions
                              *  b) are nullable
                              */
                             for (const auto& fd_determinant_expression : fd.determinants) {
                               if (!output_expressions_set.contains(fd_determinant_expression)) {
                                 return true;
                               }

                               const auto expression_idx =
                                   find_expression_idx(*fd_determinant_expression, output_expressions);
                               if (expression_idx && lqp->is_column_nullable(*expression_idx)) {
                                 return true;
                               }
                             }
                             return false;
                           }),
            fds.end());

  /**
   * Future Work: Remove redundant FDs. For example:
   *               - {a, b} => {c, SUM(d)}
   *               - {a}    => {b, c}
   *              Because we already have {a} => {c}, we do not need {a, b} => {c}. Therefore, we should change our set
   *              of FDs to the following:
   *               - {a, b} => {SUM(d)}
   *               - {a}    => {b, c}
   */
}

std::shared_ptr<AbstractLQPNode> find_diamond_origin_node(const std::shared_ptr<AbstractLQPNode>& union_root_node) {
  Assert(union_root_node->type == LQPNodeType::Union, "Expecting UnionNode as the diamond's root node.");
  DebugAssert(union_root_node->input_count() > 1, "Diamond root node does not have two inputs.");
  bool is_diamond = true;
  std::optional<std::shared_ptr<AbstractLQPNode>> diamond_origin_node;
  visit_lqp(union_root_node, [&](const auto& diamond_node) {
    if (diamond_node == union_root_node) {
      return LQPVisitation::VisitInputs;
    }
    if (!is_diamond) {
      return LQPVisitation::DoNotVisitInputs;
    }

    if (diamond_node->output_count() > 1) {
      if (!diamond_origin_node) {
        diamond_origin_node = diamond_node;
      } else {
        // The LQP traversal should always end in the same origin node having multiple outputs. Since we found two
        // different origin nodes, we abort the traversal.
        is_diamond = false;
      }
      return LQPVisitation::DoNotVisitInputs;
    }

    if (!diamond_node->left_input()) {
      // The traversal ends because we reached a MockNode, StoredTableNode or StaticTableNode. Since we did not find a
      // node with multiple outputs yet, union_root_node cannot be considered the root of a diamond.
      is_diamond = false;
    }

    return LQPVisitation::VisitInputs;
  });

  if (is_diamond && diamond_origin_node) {
    return *diamond_origin_node;
  }
  return nullptr;
}

}  // namespace hyrise
