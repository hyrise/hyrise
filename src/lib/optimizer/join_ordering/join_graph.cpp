#include "join_graph.hpp"

#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "constant_mappings.hpp"
#include "join_edge.hpp"
#include "join_plan_predicate.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace {

using namespace opossum;  // NOLINT

size_t get_vertex_idx(const std::vector<std::shared_ptr<AbstractLQPNode>>& vertices,
                      const LQPColumnReference& column_reference) {
  for (size_t vertex_idx = 0; vertex_idx < vertices.size(); ++vertex_idx) {
    if (vertices[vertex_idx]->find_output_column_id(column_reference)) return vertex_idx;
  }
  Fail("Couldn't find column " + column_reference.description() + " among vertices");
  return 0;
}

boost::dynamic_bitset<> get_predicate_vertices(const std::vector<std::shared_ptr<AbstractLQPNode>>& vertices,
                                               const std::shared_ptr<const AbstractJoinPlanPredicate>& predicate) {
  switch (predicate->type()) {
    case JoinPlanPredicateType::Atomic: {
      const auto atomic_predicate = std::static_pointer_cast<const JoinPlanAtomicPredicate>(predicate);
      boost::dynamic_bitset<> vertex_set{vertices.size()};
      vertex_set.set(get_vertex_idx(vertices, atomic_predicate->left_operand));
      if (is_lqp_column_reference(atomic_predicate->right_operand)) {
        vertex_set.set(get_vertex_idx(vertices, boost::get<LQPColumnReference>(atomic_predicate->right_operand)));
      }
      return vertex_set;
    }
    case JoinPlanPredicateType::LogicalOperator: {
      const auto logical_operand_predicate = std::static_pointer_cast<const JoinPlanLogicalPredicate>(predicate);
      return get_predicate_vertices(vertices, logical_operand_predicate->left_operand) |
             get_predicate_vertices(vertices, logical_operand_predicate->right_operand);
    }
  }
  return boost::dynamic_bitset<>{0, 0};
}
}  // namespace

namespace opossum {

JoinGraph JoinGraph::from_predicates(std::vector<std::shared_ptr<AbstractLQPNode>> vertices,
                                     std::vector<LQPParentRelation> parent_relations,
                                     const std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>>& predicates) {
  std::unordered_map<std::shared_ptr<AbstractLQPNode>, size_t> vertex_to_index;
  std::map<boost::dynamic_bitset<>, std::shared_ptr<JoinEdge>> vertices_to_edge;
  std::vector<std::shared_ptr<JoinEdge>> edges;

  for (size_t vertex_idx = 0; vertex_idx < vertices.size(); ++vertex_idx) {
    vertex_to_index[vertices[vertex_idx]] = vertex_idx;
  }

  for (const auto& predicate : predicates) {
    const auto vertex_set = get_predicate_vertices(vertices, predicate);
    auto iter = vertices_to_edge.find(vertex_set);
    if (iter == vertices_to_edge.end()) {
      auto edge = std::make_shared<JoinEdge>(vertex_set);
      iter = vertices_to_edge.emplace(vertex_set, edge).first;
      edges.emplace_back(edge);
    }
    iter->second->predicates.emplace_back(predicate);
  }

  return JoinGraph{std::move(vertices), std::move(parent_relations), std::move(edges)};
}

JoinGraph::JoinGraph(std::vector<std::shared_ptr<AbstractLQPNode>> vertices,
                     std::vector<LQPParentRelation> parent_relations, std::vector<std::shared_ptr<JoinEdge>> edges)
    : vertices(std::move(vertices)), parent_relations(std::move(parent_relations)), edges(std::move(edges)) {}

std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>> JoinGraph::find_predicates(
    const boost::dynamic_bitset<>& vertex_set) const {
  std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>> predicates;

  for (const auto& edge : edges) {
    if (edge->vertex_set != vertex_set) continue;

    for (const auto& predicate : edge->predicates) {
      predicates.emplace_back(predicate);
    }
  }

  return predicates;
}

std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>> JoinGraph::find_predicates(
    const boost::dynamic_bitset<>& vertex_set_a, const boost::dynamic_bitset<>& vertex_set_b) const {
  DebugAssert((vertex_set_a & vertex_set_b).none(), "Vertex sets are not distinct");

  std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>> predicates;

  for (const auto& edge : edges) {
    if ((edge->vertex_set & vertex_set_a).none() || (edge->vertex_set & vertex_set_b).none()) continue;
    if (!edge->vertex_set.is_subset_of(vertex_set_a | vertex_set_b)) continue;

    for (const auto& predicate : edge->predicates) {
      predicates.emplace_back(predicate);
    }
  }

  return predicates;
}

std::shared_ptr<JoinEdge> JoinGraph::find_edge(const boost::dynamic_bitset<>& vertex_set) const {
  const auto iter =
      std::find_if(edges.begin(), edges.end(), [&](const auto& edge) { return edge->vertex_set == vertex_set; });
  return iter == edges.end() ? nullptr : *iter;
}

void JoinGraph::print(std::ostream& stream) const {
  stream << "==== Vertices ====" << std::endl;
  if (vertices.empty()) {
    stream << "<none>" << std::endl;
  } else {
    for (const auto& vertex : vertices) {
      stream << vertex->description() << std::endl;
    }
  }
  stream << "===== Edges ======" << std::endl;
  if (edges.empty()) {
    stream << "<none>" << std::endl;
  } else {
    for (const auto& edge : edges) {
      edge->print(stream);
    }
  }
  std::cout << std::endl;
}

}  // namespace opossum
