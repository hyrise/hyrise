#pragma once

#include <limits>
#include <memory>
#include <optional>
#include <unordered_set>
#include <utility>
#include <vector>

#include "boost/dynamic_bitset.hpp"

#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "optimizer/join_ordering/join_plan_predicate.hpp"
#include "types.hpp"

namespace opossum {

class JoinEdge;

/**
 * Represents a connected subgraph of an LQP, with the LQPNodes "above" contained in the parent_relations and the
 * nodes "below" it contained in the vertices.
 *
 * A JoinGraph abstracts from PredicateNodes and JoinNodes and represents them as JoinEdges. It is the fundamental data
 * structure that Join Ordering algorithms operate on. See e.g. DpCcp.
 */
class JoinGraph final {
 public:
  /**
   * Converts the predicates into edges and creates a JoinGraph from them.
   */
  static JoinGraph from_predicates(std::vector<std::shared_ptr<AbstractLQPNode>> vertices,
                                   std::vector<LQPParentRelation> parent_relations,
                                   const std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>>& predicates);

  JoinGraph() = default;
  JoinGraph(std::vector<std::shared_ptr<AbstractLQPNode>> vertices, std::vector<LQPParentRelation> parent_relations, std::vector<std::shared_ptr<JoinEdge>> edges);

  /**
   * Find all predicates that use exactly the nodes in vertex set
   */
  std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>> find_predicates(
      const boost::dynamic_bitset<>& vertex_set) const;

  /**
   * Find all predicates that "connect" the two vertex sets, i.e. have operands in both of them
   */
  std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>> find_predicates(
      const boost::dynamic_bitset<>& vertex_set_a, const boost::dynamic_bitset<>& vertex_set_b) const;

  /**
   * Find the edge that exactly connects the vertices in vertex_set. Returns nullptr if no such edge exists.
   */
  std::shared_ptr<JoinEdge> find_edge(const boost::dynamic_bitset<>& vertex_set) const;

  void print(std::ostream& stream = std::cout) const;

  std::vector<std::shared_ptr<AbstractLQPNode>> vertices;
  std::vector<LQPParentRelation> parent_relations;
  std::vector<std::shared_ptr<JoinEdge>> edges;
};
}  // namespace opossum