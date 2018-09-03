#pragma once

#include <cstdint>
#include <optional>
#include <utility>
#include <vector>

#include "join_graph_edge.hpp"

namespace opossum {

class JoinGraph;

/**
 * CsgCmpPair enumeration algorithm described in "Analysis of two existing and one new dynamic programming algorithm for
 * the generation of optimal bushy join trees without cross products"
 * https://dl.acm.org/citation.cfm?id=1164207
 */
class EnumerateCcp final {
 public:
  EnumerateCcp(const size_t num_vertices, std::vector<std::pair<size_t, size_t>> edges);

  // Corresponds to EnumerateCsg in the paper
  std::vector<std::pair<JoinGraphVertexSet, JoinGraphVertexSet>> operator()();

 private:
  // Corresponds to EnumerateCsgRec in the paper
  void _enumerate_csg_recursive(std::vector<JoinGraphVertexSet>& csgs, const JoinGraphVertexSet& vertex_set,
                                const JoinGraphVertexSet& exclusion_set);

  // Corresponds to EnumerateCmp in the paper
  void _enumerate_cmp(const JoinGraphVertexSet& primary_vertex_set);

  // Corresponds to B_i(V) in the paper
  JoinGraphVertexSet _exclusion_set(const size_t vertex_idx) const;

  // Corresponds to N(S) in the paper
  JoinGraphVertexSet _neighborhood(const JoinGraphVertexSet& vertex_set, const JoinGraphVertexSet& exclusion_set) const;

  JoinGraphVertexSet _single_vertex_neighborhood(const size_t vertex_idx) const;

  // Corresponds to subset-first subset enumeration in the paper
  std::vector<JoinGraphVertexSet> _non_empty_subsets(const JoinGraphVertexSet& vertex_set) const;

  const size_t _num_vertices;
  const std::vector<std::pair<size_t, size_t>> _edges;

  std::vector<std::pair<JoinGraphVertexSet, JoinGraphVertexSet>> _csg_cmp_pairs;

  // Lookup table
  std::vector<JoinGraphVertexSet> _vertex_neighborhoods;

};

}  // namespace opossum
