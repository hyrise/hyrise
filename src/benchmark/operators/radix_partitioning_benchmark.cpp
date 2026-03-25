#include <benchmark/benchmark.h>

#include <print>

#include "uninitialized_vector.hpp"

#include "hyrise.hpp"
#include "micro_benchmark_utils.hpp"
#include "operators/join_hash/join_hash_steps.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "types.hpp"

namespace {

using namespace hyrise;  // NOLINT(build/namespaces)

template <typename T>
RadixContainer<T> generate_containers(int32_t num_elements) {
  const auto chunk_count = static_cast<uint32_t>(std::ceil(num_elements / Chunk::DEFAULT_SIZE));
  auto radix_container = RadixContainer<T>{};

  for (auto chunk_id = size_t{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto elements_per_chunk = static_cast<size_t>(num_elements / chunk_count);
    auto elements = uninitialized_vector<PartitionedElement<T>>(elements_per_chunk);
    for (auto value = T{0}; value < static_cast<int32_t>(elements_per_chunk); ++value) {
      elements[value] = PartitionedElement<T>{.row_id = RowID(ChunkID{static_cast<uint32_t>(value)}, ChunkOffset{static_cast<uint32_t>(value)}), .value = value % 3};
    }

    auto partition = Partition<T>{.elements = std::move(elements), .null_values = std::vector<bool>{}};
    radix_container.push_back(std::move(partition));
  }

  return radix_container;
}

template <typename T>
std::vector<std::vector<size_t>> compute_histograms(const RadixContainer<T>& container, uint32_t radix_bits) {
  auto histograms = std::vector(container.size(), std::vector(size_t{1} << radix_bits, size_t{0}));

  constexpr auto HASH = std::hash<T>{};
  const auto mask = (size_t{1} << radix_bits) - 1;

  for (auto chunk_id = size_t{0}; chunk_id < container.size(); ++chunk_id) {
    for (const auto& [row_id, value] : container[chunk_id].elements) {
      ++histograms[chunk_id][mask & HASH(value)];
    }
  }

  return histograms;
}

template <char variant, int locality>
void BM_Radix_Partitioning(benchmark::State& state) {
  Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());
  const auto num_elements_per_partition = state.range(0);
  const auto radix_bits = static_cast<uint64_t>(state.range(1));
  const auto partition_count = uint32_t{1} << radix_bits;
  const auto row_count = num_elements_per_partition * partition_count;
  const auto input = generate_containers<int32_t>(row_count);
  const auto histograms = compute_histograms<int32_t>(input, radix_bits);

  // for (const auto& histogram : histograms) {
  //   for (auto bin : histogram) {
  //     std::cout << " \t " << bin << "\t";
  //   }
  //   std::cout << "\n";
  // }
  //std::cout << "\n\n";
  //std::cout << std::format("num: {}, radixbits: {}, \n", num_elements_per_partition, radix_bits);
  for (auto _ : state) {
    partition_by_radix<int32_t, size_t, false, true, variant, locality>(input, histograms, radix_bits);
    // auto t = partition_by_radix<int32_t, size_t, false, true, variant, locality>(input, histograms, radix_bits);
    // for (auto part : t) {
    //     std::cout << "element counts: " << part.elements.size() << "\n";
    // }
    // break;
  }
  Hyrise::get().scheduler()->finish();
}

}  // namespace

namespace hyrise {

BENCHMARK_TEMPLATE(BM_Radix_Partitioning, 'A', 0)
    ->ArgsProduct({benchmark::CreateRange(1 << 15, 1 << 17, 2), benchmark::CreateDenseRange(7, 17, 1)});
BENCHMARK_TEMPLATE(BM_Radix_Partitioning, 'B', 0)
    ->ArgsProduct({benchmark::CreateRange(1 << 15, 1 << 17, 2), benchmark::CreateDenseRange(7, 17, 1)});
BENCHMARK_TEMPLATE(BM_Radix_Partitioning, 'C', 0)
    ->ArgsProduct({benchmark::CreateRange(1 << 15, 1 << 17, 2), benchmark::CreateDenseRange(7, 17, 1)});
BENCHMARK_TEMPLATE(BM_Radix_Partitioning, 'D', 0)
    ->ArgsProduct({benchmark::CreateRange(1 << 15, 1 << 17, 2), benchmark::CreateDenseRange(7, 17, 1)});

BENCHMARK_TEMPLATE(BM_Radix_Partitioning, 'N', -1)
    ->ArgsProduct({benchmark::CreateRange(1 << 15, 1 << 17, 2), benchmark::CreateDenseRange(7, 17, 1)});

}  // namespace hyrise
