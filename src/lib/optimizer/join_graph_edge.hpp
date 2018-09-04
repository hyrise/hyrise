#pragma once

#include <iostream>
#include <memory>

#include "boost/dynamic_bitset.hpp"
#include "boost/functional/hash.hpp"

#include "utils/assert.hpp"

namespace opossum {

class AbstractExpression;

/**
 * A bitset that represents a subsets of the vertices of a JoinGraph.
 *
 * We use a dynamic bitset here, since it has good support for operations commonly performed on join vertex sets, such
 * as union, intersection and subtraction.
 */
using JoinGraphVertexSet = boost::dynamic_bitset<>;

/**
 * Represents a (hyper)edge in a JoinGraph.
 *
 * Each predicate must operate exactly on the vertices in vertex_set. That is, each predicate must reference columns
 * from all vertices in vertex_set and no columns from vertices not in vertex_set. If the predicate wouldn't, then it
 * would belong to another edge.
 */
struct JoinGraphEdge final {
 public:
  // Doesn't check that the predicates actually only reference the vertex_set, since it has no knowledge of
  // LQPNode -> vertex index mapping. Thus, the caller has to ensure validity.
  explicit JoinGraphEdge(const JoinGraphVertexSet& vertex_set,
                         const std::vector<std::shared_ptr<AbstractExpression>>& predicates = {});

  void print(std::ostream& stream = std::cout) const;

  JoinGraphVertexSet vertex_set;
  std::vector<std::shared_ptr<AbstractExpression>> predicates;
};

}  // namespace opossum
