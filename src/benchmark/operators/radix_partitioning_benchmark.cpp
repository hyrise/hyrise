#include <benchmark/benchmark.h>

#include "uninitialized_vector.hpp"

#include "micro_benchmark_utils.hpp"
#include "operators/join_hash/join_hash_steps.hpp"
#include "types.hpp"

namespace {

using namespace hyrise;  // NOLINT(build/namespaces)

template <typename T>
RadixContainer<T> generate_container(uint32_t num_elements) {
  auto elements = uninitialized_vector<PartitionedElement<T>>(num_elements);
  for (auto value = T{0}; value < num_elements; ++value) {
    elements[value] = PartitionedElement<T>{.row_id = RowID(ChunkID{value}, ChunkOffset{value}), .value = value};
  }

  auto partition = Partition<T>{.elements = std::move(elements), .null_values = std::vector<bool>{}};
  auto radix_container = RadixContainer<T>{std::move(partition)};
  return radix_container;
}

template <typename T>
std::vector<std::vector<size_t>> compute_histogram(const RadixContainer<T>& container, uint32_t radix_bits) {
  auto histograms = std::vector(1, std::vector(size_t{1} << radix_bits, size_t{0}));

  constexpr auto HASH = std::hash<T>{};
  const auto mask = (size_t{1} << radix_bits) - 1;

  for (const auto& [row_id, value] : container[0].elements) {
    ++histograms[0][mask & HASH(value)];
  }

  return histograms;
}

template <char variant>
void BM_Radix_Partitioning(benchmark::State& state) {
  micro_benchmark_clear_cache();
  const auto num_elements = state.range(0);
  const auto radix_bits = state.range(1);
  const auto input = generate_container<uint32_t>(num_elements);
  auto histogram = compute_histogram<uint32_t>(input, radix_bits);
  const auto output = partition_by_radix<uint32_t, size_t, false, variant>(input, histogram, radix_bits);
  benchmark::DoNotOptimize(output.data());
  benchmark::ClobberMemory();
}

}  // namespace

namespace hyrise {

BENCHMARK_TEMPLATE(BM_Radix_Partitioning, 'D')
    ->ArgsProduct({benchmark::CreateRange(1e5, 1e9, 10), benchmark::CreateDenseRange(5, 17, 1)});
BENCHMARK_TEMPLATE(BM_Radix_Partitioning, 'E')
    ->ArgsProduct({benchmark::CreateRange(1e5, 1e9, 10), benchmark::CreateDenseRange(5, 17, 1)});

}  // namespace hyrise
