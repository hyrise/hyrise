#include "enumerate_ccp.hpp"

#include <set>
#include <sstream>

#include "utils/assert.hpp"

/**
 * --- Glossary ---
 *
 * Csg                  Connected Sub Graph: a set of vertices connected by edges
 * Cmp                  Complement: a connected subgraph which is connected to another connected sub graph by an edge
 * CsgCmpPair           or CCP: a pair of connected subgraphs which are connected by an edge
 * Neighborhood         of a vertex: all vertices connected to this vertex by an edge
 * Exclusion Set        of a vertex: all vertices with a lower index than this vertex
 */

namespace opossum {

EnumerateCcp::EnumerateCcp(const size_t num_vertices, std::vector<std::pair<size_t, size_t>> edges)
    : _num_vertices(num_vertices), _edges(std::move(edges)) {
  // DPccp should not be used for queries with a table count on the scale of 64 because of complexity reasons
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
   * Initialize vertex neighborhood lookup table by computing and storing the neighborhood of each vertex
   */
  _vertex_neighborhoods.resize(_num_vertices);
  for (auto vertex_idx = size_t{0}; vertex_idx < _num_vertices; ++vertex_idx) {
    _vertex_neighborhoods[vertex_idx] = _single_vertex_neighborhood(vertex_idx);
  }

  /**
   * This loop corresponds to EnumerateCsg in the paper
   *
   * It iterate from the highest to the lowest vertex index and starts a search for connected subgraphs from
   * each vertex (_enumerate_csg_recursive()).
   * For each subgraph, a search for complement subgraphs is started (_enumerate_cmp()).
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
  // subsets, i.e., that the enumeration order is correct

  std::set<JoinGraphVertexSet> enumerated_subsets;
  std::set<std::pair<JoinGraphVertexSet, JoinGraphVertexSet>> enumerated_ccps;

  for (auto csg_cmp_pair : _csg_cmp_pairs) {
    // Components must be either single-vertex or must have been enumerated as the vertex set of a previously enumerated
    // CCP
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
  /**
   * Extend `csgs` with subsets of its neighborhood, thereby forming new connected subgraphs.
   * For each newly found connected subgraph, calls itself recursively.
   */

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

void EnumerateCcp::_enumerate_cmp(const JoinGraphVertexSet& primary_vertex_set) {
  /**
   * Find complements to the connected subgraph `primary_vertex_set`
   */

  const auto exclusion_set = _exclusion_set(primary_vertex_set.find_first()) | primary_vertex_set;
  const auto neighborhood = _neighborhood(primary_vertex_set, exclusion_set);

  if (neighborhood.none()) return;

  std::vector<size_t> reverse_vertex_indices;
  auto current_vertex_idx = neighborhood.find_first();

  do {
    reverse_vertex_indices.emplace_back(current_vertex_idx);
  } while ((current_vertex_idx = neighborhood.find_next(current_vertex_idx)) != JoinGraphVertexSet::npos);

  for (auto iter = reverse_vertex_indices.rbegin(); iter != reverse_vertex_indices.rend(); ++iter) {
    auto cmp_vertex_set = JoinGraphVertexSet(_num_vertices);
    cmp_vertex_set.set(*iter);

    _csg_cmp_pairs.emplace_back(std::make_pair(primary_vertex_set, cmp_vertex_set));

    const auto extended_exclusion_set = exclusion_set | (_exclusion_set(*iter) & neighborhood);

    std::vector<JoinGraphVertexSet> csgs;
    _enumerate_csg_recursive(csgs, cmp_vertex_set, extended_exclusion_set);

    for (const auto& csg : csgs) {
      _csg_cmp_pairs.emplace_back(std::make_pair(primary_vertex_set, csg));
    }
  }
}

JoinGraphVertexSet EnumerateCcp::_exclusion_set(const size_t vertex_idx) const {
  /**
   * All vertices with an index lower than `vertex_idx`
   */

  JoinGraphVertexSet exclusion_set(_num_vertices);
  for (size_t exclusion_vertex_idx = 0; exclusion_vertex_idx < vertex_idx; ++exclusion_vertex_idx) {
    exclusion_set.set(exclusion_vertex_idx);
  }
  return exclusion_set;
}

JoinGraphVertexSet EnumerateCcp::_neighborhood(const JoinGraphVertexSet& vertex_set,
                                               const JoinGraphVertexSet& exclusion_set) const {
  /**
   * Use the lookup table `_vertex_neighborhoods[]` to find the neighborhood of a connected subgraph `vertex_set`
   */

  JoinGraphVertexSet neighborhood(_num_vertices);

  for (auto current_vertex_idx = size_t{0}; current_vertex_idx < _num_vertices; ++current_vertex_idx) {
    if (!vertex_set[current_vertex_idx]) continue;

    neighborhood |= _vertex_neighborhoods[current_vertex_idx];
  }

  return neighborhood - exclusion_set - vertex_set;
}

JoinGraphVertexSet EnumerateCcp::_single_vertex_neighborhood(const size_t vertex_idx) const {
  /**
   * Return the neighborhood of a single vertex
   */

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
  /**
   * Returns all subsets of `vertex_set`, ordered in a subsets-first order, e.g., for 01101 returns
   * {00001, 00100, 00101, 01000, 01001, 01100, 01101}
   *
   * The algorithm listed here is from stackoverflow, but I can't find the link to it anymore, hard as I tried.
   * Neither can I convincingly explain the bit-magic here, but it works nicely.
   */

  if (vertex_set.none()) return {};

  std::vector<JoinGraphVertexSet> subsets;

  const auto s = vertex_set.to_ulong();

  // s1 is the current subset subset [sic]. `s & -s` initializes it to the least significant bit in `s`. E.g., if
  // s is 011000, this initializes s1 to 001000.
  auto s1 = s & -s;

  while (s1 != s) {
    subsets.emplace_back(_num_vertices, s1);
    // This assigns to s1 the next subset of s. Somehow, this bit magic is binary incrementing s1, but only within the
    // bits set in s. So, if s is 01101 and s1 is 00101, s1 will be updated to 01000.
    s1 = s & (s1 - s);
  }
  subsets.emplace_back(vertex_set);

  return subsets;
}

}  // namespace opossum
