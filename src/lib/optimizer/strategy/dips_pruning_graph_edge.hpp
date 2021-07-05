#pragma once

#include <set>
#include <vector>

#include "expression/binary_predicate_expression.hpp"

namespace opossum {

struct DipsPruningGraphEdge {
 public:
  explicit DipsPruningGraphEdge(std::set<size_t> init_vertex_set, std::shared_ptr<BinaryPredicateExpression> predicate)
      : vertex_set(init_vertex_set) {
    predicates.push_back(predicate);
  }

  void append_predicate(std::shared_ptr<BinaryPredicateExpression> predicate) {
    // TODO(somebody): remove search when implementation "visit single node in LQP only once" is done
    if (std::find(predicates.begin(), predicates.end(), predicate) == predicates.end()) {
      predicates.push_back(predicate);
    }
  }

  bool connects_vertex(size_t vertex) { return vertex_set.find(vertex) != vertex_set.end(); }

  size_t neighbour(size_t vertex) {
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
