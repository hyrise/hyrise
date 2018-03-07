#include "join_graph_builder.hpp"

#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "utils/assert.hpp"

namespace opossum {

std::shared_ptr<JoinGraph> JoinGraphBuilder::operator()(const std::shared_ptr<AbstractLQPNode>& lqp) {
  /**
   * Traverse the LQP until the first non-vertex type (e.g. a UnionNode) is found or a node doesn't have precisely
   * one input. This way, we traverse past Sort/Aggregate etc. nodes that later form the "outputs" of the JoinGraph
   */
  auto current_node = lqp;
  while (_lqp_node_type_is_vertex(current_node->type())) {
    if (!current_node->left_input() || current_node->right_input()) {
      break;
    }

    current_node = current_node->left_input();
  }
  const auto output_relations = current_node->output_relations();

  // Traverse the LQP, identifying JoinPlanPredicates and Vertices
  _traverse(current_node);

  return JoinGraph::from_predicates(std::move(_vertices), std::move(output_relations), _predicates);
}

void JoinGraphBuilder::_traverse(const std::shared_ptr<AbstractLQPNode>& node) {
  // Makes it possible to call _traverse() on inputren without checking whether they exist first.
  if (!node) {
    return;
  }

  if (_lqp_node_type_is_vertex(node->type())) {
    _vertices.emplace_back(node);
    return;
  }

  switch (node->type()) {
    case LQPNodeType::Join: {
      /**
       * Cross joins are simply being traversed past. Outer joins are hard to address during JoinOrdering and until we
       * do, outer join predicates are not included in the JoinGraph.
       * The outer join node is added as a vertex and traversal stops at this point.
       */

      const auto join_node = std::static_pointer_cast<JoinNode>(node);

      if (join_node->join_mode() == JoinMode::Inner) {
        _predicates.emplace_back(std::make_shared<JoinPlanAtomicPredicate>(
            join_node->join_column_references()->first, *join_node->predicate_condition(),
            join_node->join_column_references()->second));
      }

      if (join_node->join_mode() == JoinMode::Inner || join_node->join_mode() == JoinMode::Cross) {
        _traverse(node->left_input());
        _traverse(node->right_input());
      } else {
        _vertices.emplace_back(node);
      }
    } break;

    case LQPNodeType::Predicate: {
      /**
       * BETWEEN PredicateNodes are turned into two predicates, because JoinPlanPredicates do not support BETWEEN. All
       * other PredicateConditions produce exactly one JoinPlanPredicate
       */

      const auto predicate_node = std::static_pointer_cast<PredicateNode>(node);

      if (predicate_node->value2()) {
        DebugAssert(predicate_node->predicate_condition() == PredicateCondition::Between, "Expected between");

        _predicates.emplace_back(std::make_shared<JoinPlanAtomicPredicate>(
            predicate_node->column_reference(), PredicateCondition::GreaterThanEquals, predicate_node->value()));

        _predicates.emplace_back(std::make_shared<JoinPlanAtomicPredicate>(
            predicate_node->column_reference(), PredicateCondition::LessThanEquals, *predicate_node->value2()));
      } else {
        _predicates.emplace_back(std::make_shared<JoinPlanAtomicPredicate>(
            predicate_node->column_reference(), predicate_node->predicate_condition(), predicate_node->value()));
      }

      _traverse(node->left_input());
    } break;

    case LQPNodeType::Union: {
      /**
       * A UnionNode is the entry point to disjunction, which is parsed starting from _parse_union(). Normal traversal
       * is commenced from the node "below" the Union.
       */

      const auto union_node = std::static_pointer_cast<UnionNode>(node);

      if (union_node->union_mode() == UnionMode::Positions) {
        const auto parse_result = _parse_union(union_node);

        _traverse(parse_result.base_node);
        _predicates.emplace_back(parse_result.predicate);
      } else {
        _vertices.emplace_back(node);
      }
    } break;

    default: { Fail("Node type not suited for JoinGraph"); }
  }
}

JoinGraphBuilder::PredicateParseResult JoinGraphBuilder::_parse_predicate(
    const std::shared_ptr<AbstractLQPNode>& node) const {
  if (node->type() == LQPNodeType::Predicate) {
    const auto predicate_node = std::static_pointer_cast<const PredicateNode>(node);

    std::shared_ptr<const AbstractJoinPlanPredicate> left_predicate;

    if (predicate_node->value2()) {
      DebugAssert(predicate_node->predicate_condition() == PredicateCondition::Between, "Expected between");

      left_predicate = std::make_shared<JoinPlanLogicalPredicate>(
          std::make_shared<JoinPlanAtomicPredicate>(predicate_node->column_reference(),
                                                    PredicateCondition::GreaterThanEquals, predicate_node->value()),
          JoinPlanPredicateLogicalOperator::And,
          std::make_shared<JoinPlanAtomicPredicate>(predicate_node->column_reference(),
                                                    PredicateCondition::LessThanEquals, *predicate_node->value2()));
    } else {
      left_predicate = std::make_shared<JoinPlanAtomicPredicate>(
          predicate_node->column_reference(), predicate_node->predicate_condition(), predicate_node->value());
    }

    const auto base_node = predicate_node->left_input();

    if (base_node->output_count() > 1) {
      return {base_node, left_predicate};
    } else {
      const auto parse_result_right = _parse_predicate(base_node);

      const auto and_predicate = std::make_shared<JoinPlanLogicalPredicate>(
          left_predicate, JoinPlanPredicateLogicalOperator::And, parse_result_right.predicate);

      return {parse_result_right.base_node, and_predicate};
    }
  } else if (node->type() == LQPNodeType::Union) {
    return _parse_union(std::static_pointer_cast<UnionNode>(node));
  } else {
    Fail("Unexpected node type");
    return {nullptr, nullptr};
  }
}

JoinGraphBuilder::PredicateParseResult JoinGraphBuilder::_parse_union(
    const std::shared_ptr<UnionNode>& union_node) const {
  DebugAssert(union_node->left_input() && union_node->right_input(),
              "UnionNode needs both inputren set in order to be parsed");

  const auto parse_result_left = _parse_predicate(union_node->left_input());
  const auto parse_result_right = _parse_predicate(union_node->right_input());

  DebugAssert(parse_result_left.base_node == parse_result_right.base_node, "Invalid OR not having a single base node");

  const auto or_predicate = std::make_shared<JoinPlanLogicalPredicate>(
      parse_result_left.predicate, JoinPlanPredicateLogicalOperator::Or, parse_result_right.predicate);

  return {parse_result_left.base_node, or_predicate};
}

bool JoinGraphBuilder::_lqp_node_type_is_vertex(const LQPNodeType node_type) const {
  return node_type != LQPNodeType::Join && node_type != LQPNodeType::Union && node_type != LQPNodeType::Predicate;
}
}  // namespace opossum
