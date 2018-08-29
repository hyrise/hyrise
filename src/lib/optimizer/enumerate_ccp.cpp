#include "enumerate_ccp.hpp"

#include <set>
#include <sstream>

#include "utils/assert.hpp"

namespace opossum {

EnumerateCcp::EnumerateCcp(const size_t num_vertices, std::vector<std::pair<size_t, size_t>> edges)
    : _num_vertices(num_vertices), _edges(std::move(edges)) {
  Assert(num_vertices < sizeof(unsigned long) * 8, "Too many vertices, EnumerateCcp relies on to_ulong()");  // NOLINT

#if IS_DEBUG
  // Test the input data for validity, i.e. whether all mentioned vertex indices in the edges are smaller than
  // _num_vertices
  for (const auto& edge : _edges) {
    Assert(edge.first < _num_vertices && edge.second < _num_vertices, "Vertex Index out of range");
  }
#endif
}

std::vector<std::pair<JoinGraphVertexSet, JoinGraphVertexSet>> EnumerateCcp::operator()() {
  /**
   * Initialize vertex neighborhood lookup table
   */
  _vertex_neighborhoods.resize(_num_vertices);
  for (auto vertex_idx = size_t{0}; vertex_idx < _num_vertices; ++vertex_idx) {
    _vertex_neighborhoods[vertex_idx] = _single_vertex_neighborhood(vertex_idx);
  }

  /**
   * This loop corresponds to EnumerateCsg in the paper
   * Iterate fro the highest to the lowest vertex index
   */
  for (size_t reverse_vertex_idx = 0; reverse_vertex_idx < _num_vertices; ++reverse_vertex_idx) {
    const auto forward_vertex_idx = _num_vertices - reverse_vertex_idx - 1;

    auto start_vertex_set = JoinGraphVertexSet(_num_vertices);
    start_vertex_set.set(forward_vertex_idx);
    _enumerate_cmp(start_vertex_set);

    std::vector<JoinGraphVertexSet> csgs;
    _enumerate_csg_recursive(csgs, start_vertex_set, _exclusion_set(forward_vertex_idx));
    for (const auto& csg : csgs) {
      _enumerate_cmp(csg);
    }
  }

#if IS_DEBUG
  // Assert that the algorithm didn't create duplicates and that all created ccps contain only previously enumerated
  // subsets

  std::set<JoinGraphVertexSet> enumerated_subsets;
  std::set<std::pair<JoinGraphVertexSet, JoinGraphVertexSet>> enumerated_ccps;

  for (auto csg_cmp_pair : _csg_cmp_pairs) {
    // Components must be either single-vertex or must have been enumerated as the vertex set of a previously CCP
    Assert(csg_cmp_pair.first.count() == 1 || enumerated_subsets.count(csg_cmp_pair.first) != 0,
           "CSG not yet enumerated");
    Assert(csg_cmp_pair.second.count() == 1 || enumerated_subsets.count(csg_cmp_pair.second) != 0,
           "CSG not yet enumerated");

    enumerated_subsets.emplace(csg_cmp_pair.first | csg_cmp_pair.second);

    Assert(enumerated_ccps.emplace(csg_cmp_pair).second, "Duplicate CCP was generated");
    std::swap(csg_cmp_pair.first, csg_cmp_pair.second);
    Assert(enumerated_ccps.emplace(csg_cmp_pair).second, "Duplicate CCP was generated");
  }
#endif

  return _csg_cmp_pairs;
}

void EnumerateCcp::_enumerate_csg_recursive(std::vector<JoinGraphVertexSet>& csgs, const JoinGraphVertexSet& vertex_set,
                                            const JoinGraphVertexSet& exclusion_set) {
  const auto neighborhood = _neighborhood(vertex_set, exclusion_set);
  const auto neighborhood_subsets = _non_empty_subsets(neighborhood);
  const auto extended_exclusion_set = exclusion_set | neighborhood;

  for (const auto& subset : neighborhood_subsets) {
    csgs.emplace_back(subset | vertex_set);
  }

  for (const auto& subset : neighborhood_subsets) {
    _enumerate_csg_recursive(csgs, subset | vertex_set, extended_exclusion_set);
  }
}

void EnumerateCcp::_enumerate_cmp(const JoinGraphVertexSet& vertex_set) {
  const auto exclusion_set = _exclusion_set(vertex_set.find_first()) | vertex_set;
  const auto neighborhood = _neighborhood(vertex_set, exclusion_set);

  if (neighborhood.none()) return;

  std::vector<size_t> reverse_vertex_indices;
  auto current_vertex_idx = neighborhood.find_first();

  do {
    reverse_vertex_indices.emplace_back(current_vertex_idx);
  } while ((current_vertex_idx = neighborhood.find_next(current_vertex_idx)) != JoinGraphVertexSet::npos);

  for (auto iter = reverse_vertex_indices.rbegin(); iter != reverse_vertex_indices.rend(); ++iter) {
    auto cmp_vertex_set = JoinGraphVertexSet(_num_vertices);
    cmp_vertex_set.set(*iter);

    _csg_cmp_pairs.emplace_back(std::make_pair(vertex_set, cmp_vertex_set));

    const auto extended_exclusion_set = exclusion_set | (_exclusion_set(*iter) & neighborhood);

    std::vector<JoinGraphVertexSet> csgs;
    _enumerate_csg_recursive(csgs, cmp_vertex_set, extended_exclusion_set);

    for (const auto& csg : csgs) {
      _csg_cmp_pairs.emplace_back(std::make_pair(vertex_set, csg));
    }
  }
}

JoinGraphVertexSet EnumerateCcp::_exclusion_set(const size_t vertex_idx) const {
  JoinGraphVertexSet exclusion_set(_num_vertices);
  for (size_t exclusion_vertex_idx = 0; exclusion_vertex_idx < vertex_idx; ++exclusion_vertex_idx) {
    exclusion_set.set(exclusion_vertex_idx);
  }
  return exclusion_set;
}

JoinGraphVertexSet EnumerateCcp::_neighborhood(const JoinGraphVertexSet& vertex_set,
                                               const JoinGraphVertexSet& exclusion_set) const {
  JoinGraphVertexSet neighborhood(_num_vertices);

  for (auto current_vertex_idx = size_t{0}; current_vertex_idx < _num_vertices; ++current_vertex_idx) {
    if (!vertex_set[current_vertex_idx]) continue;

    neighborhood |= _vertex_neighborhoods[current_vertex_idx];
  }

  return neighborhood - exclusion_set - vertex_set;
}

JoinGraphVertexSet EnumerateCcp::_single_vertex_neighborhood(const size_t vertex_idx) const {
  JoinGraphVertexSet neighbourhood(_num_vertices);
  for (const auto& edge : _edges) {
    if (vertex_idx == edge.first && vertex_idx != edge.second) {
      neighbourhood.set(edge.second);
    }
    if (vertex_idx != edge.first && vertex_idx == edge.second) {
      neighbourhood.set(edge.first);
    }
  }

  return neighbourhood;
}

std::vector<JoinGraphVertexSet> EnumerateCcp::_non_empty_subsets(const JoinGraphVertexSet& vertex_set) const {
  if (vertex_set.none()) return {};

  std::vector<JoinGraphVertexSet> subsets;

  const auto s = vertex_set.to_ulong();
  auto s1 = s & -s;

  while (s1 != s) {
    subsets.emplace_back(_num_vertices, s1);
    s1 = s & (s1 - s);
  }
  subsets.emplace_back(vertex_set);

  return subsets;
}

}  // namespace opossum
