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

template <bool reduce_tlb_pressure>
void BM_Radix_Partitioning(benchmark::State& state) {
  const auto num_elements_per_partition = state.range(0);
  const auto radix_bits = static_cast<uint64_t>(state.range(1));
  const auto input = generate_container<uint32_t>(num_elements_per_partition * (1ull << radix_bits));
  const auto histogram = compute_histogram<uint32_t>(input, radix_bits);
  for (auto _ : state) {
    partition_by_radix<uint32_t, size_t, false, reduce_tlb_pressure>(input, histogram, radix_bits);
  }
}

}  // namespace

namespace hyrise {

BENCHMARK_TEMPLATE(BM_Radix_Partitioning, true)
    ->ArgsProduct({benchmark::CreateRange(1e4, 1e6, 10), benchmark::CreateDenseRange(1, 10, 1)});
BENCHMARK_TEMPLATE(BM_Radix_Partitioning, false)
    ->ArgsProduct({benchmark::CreateRange(1e4, 1e6, 10), benchmark::CreateDenseRange(1, 10, 1)});

}  // namespace hyrise
