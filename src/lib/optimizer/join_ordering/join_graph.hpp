#pragma once

#include <limits>
#include <memory>
#include <optional>
#include <unordered_set>
#include <utility>
#include <vector>

#include "join_vertex_set.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "optimizer/join_ordering/join_plan_predicate.hpp"
#include "types.hpp"

namespace opossum {

class JoinEdge;

/**
 * Represents a connected subgraph of an LQP, with the LQPNodes "above" contained in the output_relations and the
 * subplans "below" it being the vertices.
 * The JoinGraph clusters Predicates operating on the same set of vertices into JoinEdges.
 *
 * In opposition to the LQP, the `JoinGraph` has no notion of the order in which Predicates and Joins (which are really
 * just Predicates as well, as far as the `JoinGraph` is concerned) are performed. Instead, it provides facilities
 * to identify all Predicates that operate on a set of Vertices (i.e. LQP nodes) - the find_predicates() functions.
 *
 * A JoinGraph abstracts from PredicateNodes and JoinNodes and represents them as JoinEdges. It is the fundamental data
 * structure that Join Ordering algorithms operate on. See e.g. DpCcp.
 */
class JoinGraph final {
 public:
  /**
   * Return the first JoinGraph found when recursively traversing the @param lqp
   */
  static std::shared_ptr<JoinGraph> from_lqp(const std::shared_ptr<AbstractLQPNode>& lqp);

  /**
   * Converts the predicates into edges and creates a JoinGraph from them.
   */
  static std::shared_ptr<JoinGraph> from_predicates(
      std::vector<std::shared_ptr<AbstractLQPNode>> vertices, std::vector<LQPOutputRelation> output_relations,
      const std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>>& predicates);

  JoinGraph() = default;
  JoinGraph(std::vector<std::shared_ptr<AbstractLQPNode>> vertices, std::vector<LQPOutputRelation> output_relations,
            std::vector<std::shared_ptr<JoinEdge>> edges);

  /**
   * Find all predicates that use exactly the nodes in vertex set
   */
  std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>> find_predicates(const JoinVertexSet& vertex_set) const;

  /**
   * Find all predicates that "connect" the two vertex sets, i.e. have operands in both of them
   */
  std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>> find_predicates(
      const JoinVertexSet& vertex_set_a, const JoinVertexSet& vertex_set_b) const;

  /**
   * Find the edge that exactly connects the vertices in vertex_set. Returns nullptr if no such edge exists.
   */
  std::shared_ptr<JoinEdge> find_edge(const JoinVertexSet& vertex_set) const;

  void print(std::ostream& stream = std::cout) const;

  std::vector<std::shared_ptr<AbstractLQPNode>> vertices;
  std::vector<LQPOutputRelation> output_relations;
  std::vector<std::shared_ptr<JoinEdge>> edges;
};
}  // namespace opossum
