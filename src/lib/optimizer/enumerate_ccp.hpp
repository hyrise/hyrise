#pragma once

#define NDEBUG 1

#include <cstdint>
#include <optional>
#include <utility>
#include <vector>

#include "join_graph_edge.hpp"

namespace opossum {

class JoinGraph;

class EnumerateCcp final {
 public:
  EnumerateCcp(size_t num_vertices, std::vector<std::pair<size_t, size_t>> edges);

  std::vector<std::pair<JoinGraphVertexSet, JoinGraphVertexSet>> operator()();

 private:
  size_t _num_vertices;
  std::vector<std::pair<size_t, size_t>> _edges;
  std::vector<std::pair<JoinGraphVertexSet, boost::dynamic_bitset<>>> _csg_cmp_pairs;

  std::vector<boost::dynamic_bitset<>> _vertex_neighbourhoods;

  void _enumerate_csg_recursive(std::vector<boost::dynamic_bitset<>>& csgs, const JoinGraphVertexSet& vertex_set,
                                const JoinGraphVertexSet& exclusion_set);
  void _enumerate_cmp(const JoinGraphVertexSet& vertex_set);
  JoinGraphVertexSet _exclusion_set(const size_t vertex_idx) const;
  JoinGraphVertexSet _neighbourhood(const JoinGraphVertexSet& vertex_set,
                                         const JoinGraphVertexSet& exclusion_set) const;
  JoinGraphVertexSet _neighbourhood2(const JoinGraphVertexSet& vertex_set,
                                          const JoinGraphVertexSet& exclusion_set) const;
  std::vector<boost::dynamic_bitset<>> _non_empty_subsets(const JoinGraphVertexSet& vertex_set) const;
};

std::vector<std::pair<size_t, size_t>> enumerate_ccp_edges_from_join_graph(const JoinGraph& join_graph);

}  // namespace opossum
