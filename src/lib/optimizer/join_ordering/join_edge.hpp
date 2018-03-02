#pragma once

#include <iostream>
#include <ostream>

#include "join_vertex_set.hpp"

namespace opossum {

class AbstractJoinPlanPredicate;

/**
 * Represents a (hyper)edge in a JoinGraph.
 *
 * Each predicate must operate exactly on the vertices in vertex_set. That is, each predicate must reference columns
 * from all vertices in vertex_set and no columns from vertices not in vertex_set. If the predicate wouldn't, then it
 * would belong to another edge.
 */
class JoinEdge final {
 public:
  explicit JoinEdge(const JoinVertexSet& vertex_set);

  void print(std::ostream& stream = std::cout) const;

  const JoinVertexSet vertex_set;
  std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>> predicates;
};

}  // namespace opossum
