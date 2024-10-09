#include <iostream>

#include <boost/dynamic_bitset.hpp>

#include "optimizer/join_ordering/join_graph_edge.hpp"
#include "types.hpp"
#include "utils/timer.hpp"

using namespace hyrise;  // NOLINT(build/namespaces)

int main() {
  const auto world = pmr_string{"world"};
  std::cout << "Hello " << world << "!\n";

  constexpr auto NUM_REPETITIONS = 100'000;
  constexpr auto vertex_count = 63;
  auto overall_count = size_t{0};

  auto timer1 = Timer{};
  for (auto i = 0; i < NUM_REPETITIONS; ++i) {
    for (auto vertex_idx = vertex_count; vertex_idx > 0; --vertex_idx) {
      auto start_vertex_set = JoinGraphVertexSet(vertex_count);

      start_vertex_set.set(vertex_idx - 1);
      // Set some random bits so `reset()` does something.
      for (auto pos = 0; pos < vertex_count; ++pos) {
        if (pos % 3 == 0) {
          start_vertex_set.set(pos);
        }
      }

      overall_count += start_vertex_set.count();
    }
  }

  std::cout << timer1.lap_formatted() << "  " << overall_count << "\n";
  overall_count = size_t{0};

  auto timer2 = Timer{};
  for (auto i = 0; i < NUM_REPETITIONS; ++i) {
    auto start_vertex_set = JoinGraphVertexSet(vertex_count);
    for (auto vertex_idx = vertex_count; vertex_idx > 0; --vertex_idx) {
      start_vertex_set.reset();

      start_vertex_set.set(vertex_idx - 1);
      // Set some random bits so `reset()` does something.
      for (auto pos = 0; pos < vertex_count; ++pos) {
        if (pos % 3 == 0) {
          start_vertex_set.set(pos);
        }
      }

      overall_count += start_vertex_set.count();
    }
  }

  std::cout << timer2.lap_formatted() << "  " << overall_count << "\n";

  return 0;
}
