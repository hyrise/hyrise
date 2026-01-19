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
    elements[value] = PartitionedElement<T>{.value = value, .row_id = RowID(ChunkID{value}, ChunkOffset{value})};
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

  for (const auto& [value, row_id] : container[0].elements) {
    ++histograms[0][mask & HASH(value)];
  }

  return histograms;
}

template <char variant, int locality>
void BM_Radix_Partitioning(benchmark::State& state) {
  const auto num_elements_per_partition = state.range(0);
  const auto radix_bits = static_cast<uint64_t>(state.range(1));
  const auto input = generate_container<uint32_t>(num_elements_per_partition * (1ull << radix_bits));
  const auto histogram = compute_histogram<uint32_t>(input, radix_bits);
  for (auto _ : state) {
    partition_by_radix<uint32_t, size_t, false, true, variant, locality>(input, histogram, radix_bits);
  }
}

}  // namespace

namespace hyrise {

BENCHMARK_TEMPLATE(BM_Radix_Partitioning, 'A', 0)
    ->ArgsProduct({benchmark::CreateRange(5e2, 5e4, 10), benchmark::CreateDenseRange(7, 17, 1)});
BENCHMARK_TEMPLATE(BM_Radix_Partitioning, 'B', 0)
    ->ArgsProduct({benchmark::CreateRange(5e2, 5e4, 10), benchmark::CreateDenseRange(7, 17, 1)});
BENCHMARK_TEMPLATE(BM_Radix_Partitioning, 'C', 0)
    ->ArgsProduct({benchmark::CreateRange(5e2, 5e4, 10), benchmark::CreateDenseRange(7, 17, 1)});
BENCHMARK_TEMPLATE(BM_Radix_Partitioning, 'D', 0)
    ->ArgsProduct({benchmark::CreateRange(5e2, 5e4, 10), benchmark::CreateDenseRange(7, 17, 1)});

BENCHMARK_TEMPLATE(BM_Radix_Partitioning, 'A', 1)
    ->ArgsProduct({benchmark::CreateRange(5e2, 5e4, 10), benchmark::CreateDenseRange(7, 17, 1)});
BENCHMARK_TEMPLATE(BM_Radix_Partitioning, 'B', 1)
    ->ArgsProduct({benchmark::CreateRange(5e2, 5e4, 10), benchmark::CreateDenseRange(7, 17, 1)});
BENCHMARK_TEMPLATE(BM_Radix_Partitioning, 'C', 1)
    ->ArgsProduct({benchmark::CreateRange(5e2, 5e4, 10), benchmark::CreateDenseRange(7, 17, 1)});
BENCHMARK_TEMPLATE(BM_Radix_Partitioning, 'D', 1)
    ->ArgsProduct({benchmark::CreateRange(5e2, 5e4, 10), benchmark::CreateDenseRange(7, 17, 1)});

BENCHMARK_TEMPLATE(BM_Radix_Partitioning, 'N', -1)
    ->ArgsProduct({benchmark::CreateRange(5e2, 5e4, 10), benchmark::CreateDenseRange(7, 17, 1)});

}  // namespace hyrise
