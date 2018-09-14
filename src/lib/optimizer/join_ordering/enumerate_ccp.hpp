#pragma once

#include <cstdint>
#include <optional>
#include <utility>
#include <vector>

#include "join_graph_edge.hpp"

namespace opossum {

class JoinGraph;

/**
 * A CsgCmpPair is a pair of connected subgraphs that are directly connected by an edge.
 *
 * In JoinOrdering, a CsgCmpPair represents a candidate join operation.
 */
using CsgCmpPair = std::pair<JoinGraphVertexSet, JoinGraphVertexSet>;

/**
 * CsgCmpPair ("CCP") enumeration algorithm described in "Analysis of two existing and one new dynamic programming
 * algorithm for the generation of optimal bushy join trees without cross products"
 * https://dl.acm.org/citation.cfm?id=1164207
 *
 * Input: A JoinGraph in the form of a number of vertices and edges as a set of index pairs
 *
 * Output: A list of CsgCmpPair, in an order suitable for dynamic programming
 *
 *
 * ### Example
 *
 * Consider this chain-shaped JoinGraph: 0 <-> 1 <-> 2 <-> 3
 *
 * The following CsgCmpPairs are enumerated, where the bit at 2^i represents the vertex i:
 * (0b0100, 0b1000)
 * (0b0010, 0b0100)
 * (0b0010, 0b1100)
 * (0b0110, 0b1000)
 * (0b0001, 0b0010)
 * (0b0001, 0b0110)
 * (0b0001, 0b1110)
 * (0b0011, 0b0100)
 * (0b0011, 0b1100)
 * (0b0111, 0b1000)
 *
 * Notice how:
 *   -> these are all possible subdivisions of the JoinGraph into two connected subgraphs that are connected by an edge
 *      (aka "CsgCmpPair)
 *   -> The order of the CsgCmpPairs is so that a component (both Csg and Cmp are "components") is either
 *          -> a single vertex or
 *          -> a subgraph for which **all possible subdivisions have been enumerated before**. This fact is essential
 *              for dynamic programming to work.
 */
class EnumerateCcp final {
 public:
  EnumerateCcp(const size_t num_vertices, std::vector<std::pair<size_t, size_t>> edges);

  // Corresponds to EnumerateCsg in the paper
  std::vector<CsgCmpPair> operator()();

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
