#pragma once

#include <set>
#include <vector>

#include "expression/binary_predicate_expression.hpp"

namespace opossum {

struct DipsPruningGraphEdge {
 public:
  explicit DipsPruningGraphEdge(const std::set<size_t> init_vertex_set,
                                const std::shared_ptr<BinaryPredicateExpression> predicate)
      : vertex_set(init_vertex_set) {
    predicates.push_back(predicate);
  }

  bool operator==(const DipsPruningGraphEdge& edge_to_compare) const {
    return (vertex_set == edge_to_compare.vertex_set && predicates == edge_to_compare.predicates);
  }

  void append_predicate(const std::shared_ptr<BinaryPredicateExpression> predicate) {
    // TODO(somebody): remove search when implementation "visit single node in LQP only once" is done
    if (std::find(predicates.begin(), predicates.end(), predicate) == predicates.end()) {
      predicates.push_back(predicate);
    }
  }

  bool connects_vertex(const size_t vertex) { return vertex_set.find(vertex) != vertex_set.end(); }

  size_t neighbour(const size_t vertex) {
    for (auto neighbour : vertex_set) {
      if (neighbour != vertex) {
        return neighbour;
      }
    }
    Assert(false, "There always should be a neighbor");
  }

  std::set<size_t> vertex_set;
  std::vector<std::shared_ptr<BinaryPredicateExpression>> predicates;
};

}  // namespace opossum
