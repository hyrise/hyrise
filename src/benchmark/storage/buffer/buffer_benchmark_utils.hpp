#pragma once

#if HYRISE_NUMA_SUPPORT
#include <numa.h>
#include <numaif.h>
#endif
#ifdef __AVX512VL__
#include <x86intrin.h>
#endif
#include <sys/mman.h>
#include <unistd.h>
#include <boost/container/pmr/memory_resource.hpp>
#include <random>
#include "hyrise.hpp"
#include "storage/buffer/buffer_manager.hpp"
#include "utils/assert.hpp"
#include "zipfian_int_distribution.hpp"

namespace hyrise {

constexpr static auto SEED = 123761253768512;

constexpr static auto CACHE_LINE_SIZE = 64;

inline std::byte* mmap_region(const size_t num_bytes) {
#ifdef __APPLE__
  const int flags = MAP_PRIVATE | MAP_ANON | MAP_NORESERVE;
#elif __linux__
  const int flags = MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE;
#endif
  const auto mapped_memory = static_cast<std::byte*>(mmap(NULL, num_bytes, PROT_READ | PROT_WRITE, flags, -1, 0));

  if (mapped_memory == MAP_FAILED) {
    const auto error = errno;
    Fail("Failed to map volatile pool region: " + strerror(error));
  }

  return mapped_memory;
}

inline int get_numa_node(void* addr) {
  int numa_node = -1;

#if HYRISE_NUMA_SUPPORT
  if (get_mempolicy(&numa_node, NULL, 0, addr, MPOL_F_NODE | MPOL_F_ADDR) != 0) {
    Fail("Failed to get numa node");
  }
#endif
  return numa_node;
}

inline void munmap_region(std::byte* region, const size_t num_bytes) {
  if (munmap(region, num_bytes) < 0) {
    const auto error = errno;
    Fail("Failed to unmap volatile pool region: " + strerror(error));
  }
}

inline void explicit_move_pages(void* mem, size_t size, int node) {
#if HYRISE_NUMA_SUPPORT
  auto nodes = numa_allocate_nodemask();
  numa_bitmask_setbit(nodes, node);
  if (mbind(mem, size, MPOL_BIND, nodes ? nodes->maskp : NULL, nodes ? nodes->size + 1 : 0,
            MPOL_MF_MOVE | MPOL_MF_STRICT) != 0) {
    numa_bitmask_free(nodes);

    Fail("Move pages failed");
  }
  numa_bitmask_free(nodes);
#endif
}

inline void simulate_cacheline_store(std::byte* ptr) {
#ifdef __AVX512VL__
  // using a non-temporal memory hint
  _mm512_stream_si512(reinterpret_cast<__m512*>(ptr), _mm512_set1_epi8(0x1));
#else
  std::memset(ptr, 0x1, CACHE_LINE_SIZE);
#endif
}

inline void simulate_cacheline_read(std::byte* ptr) {
#ifdef __AVX512VL__
  auto v = _mm512_load_si512(ptr);
  benchmark::DoNotOptimize(v);
#endif
}

inline void simulate_page_read(std::byte* ptr, int num_bytes) {
  for (int i = 0; i < num_bytes; i += CACHE_LINE_SIZE) {
    simulate_cacheline_read(ptr + i);
  }
}

inline void simulate_store(std::byte* ptr, int num_bytes) {
  for (int i = 0; i < num_bytes; i += CACHE_LINE_SIZE) {
    simulate_cacheline_store(ptr + i);
  }
}

enum class YCSBTableAccessPattern : int { ReadMostly = 90, Balanced = 50, WriteHeavy = 10 };
enum class YSCBOperationType : int { Read, Update };
enum class YCSBTupleSize : uint32_t { Small = CACHE_LINE_SIZE, Medium = 512, Large = 4096, Huge = 32768 };

using YCSBTuple = std::pair<YCSBTupleSize, std::byte*>;

using YCSBKey = uint64_t;
using YCSBTable = std::vector<YCSBTuple>;
using YSCBOperation = std::pair<YCSBKey, YSCBOperationType>;
using YCSBOperations = std::vector<YSCBOperation>;

template <size_t DatabaseSizeBytes>
inline YCSBTable generate_ycsb_table(boost::container::pmr::memory_resource* memory_resource) {
  std::mt19937 generator{std::random_device{}()};
  std::uniform_int_distribution<int> distribution(0, magic_enum::enum_count<YCSBTupleSize>() - 1);
  auto table = std::vector<YCSBTuple>{};
  size_t current_size = 0;
  auto& buffer_manager = Hyrise::get().buffer_manager;
  while (current_size < DatabaseSizeBytes) {
    auto tuple_size = magic_enum::enum_value<YCSBTupleSize>(distribution(generator));
    auto ptr = memory_resource->allocate(static_cast<size_t>(tuple_size), 64);
    buffer_manager.pin_exclusive(buffer_manager.find_page(ptr));
    simulate_store(reinterpret_cast<std::byte*>(ptr), static_cast<int>(tuple_size));
    buffer_manager.set_dirty(buffer_manager.find_page(ptr));
    buffer_manager.unpin_exclusive(buffer_manager.find_page(ptr));
    auto tuple = std::make_pair(tuple_size, reinterpret_cast<std::byte*>(ptr));
    table.push_back(tuple);
    current_size += static_cast<size_t>(tuple_size);
  }

  return table;
}

template <YCSBTableAccessPattern accessPattern, size_t NumOperations>
inline YCSBOperations generate_ycsb_operations(const size_t num_keys, const float zipf_skew) {
  YCSBOperations ops;
  static thread_local std::mt19937 generator{std::random_device{}()};
  std::uniform_int_distribution<int> op_distribution(0, 100);
  zipfian_int_distribution<YCSBKey> key_distribution{0, static_cast<YCSBKey>(num_keys - 1), zipf_skew};
  for (auto i = 0; i < NumOperations; i++) {
    auto op = op_distribution(generator) < static_cast<int>(accessPattern) ? YSCBOperationType::Read
                                                                           : YSCBOperationType::Update;
    auto key = static_cast<YCSBKey>(key_distribution(generator));
    ops.push_back(std::make_pair(key, op));
  }

  return ops;
}

inline void execute_ycsb_action(const YCSBTable& table, BufferManager& buffer_manager, const YSCBOperation operation) {
  auto [key, op_type] = operation;
  const auto& tuple = table[key];
  auto num_bytes = static_cast<int>(tuple.first);
  auto ptr = tuple.second;
  auto page_id = buffer_manager.find_page(ptr);
  if (op_type == YSCBOperationType::Read) {
    buffer_manager.pin_shared(page_id, AccessIntent::Read);
    simulate_page_read(ptr, num_bytes);
    buffer_manager.unpin_shared(page_id);
  } else {
    buffer_manager.pin_exclusive(page_id);
    simulate_store(ptr, num_bytes);
    buffer_manager.unpin_exclusive(page_id);
  }
}

inline void warmup(YCSBTable& table, BufferManager& buffer_manager) {
  std::mt19937 gen{std::random_device{}()};
  std::uniform_int_distribution<YCSBKey> dist(0, table.size() - 1);

  int unused_bytes = buffer_manager.config().dram_buffer_pool_size + buffer_manager.config().numa_buffer_pool_size;
  while (unused_bytes > 0) {
    const auto key = dist(gen);
    auto [size, _] = table[key];
    YSCBOperation op = std::make_pair(key, YSCBOperationType::Read);
    execute_ycsb_action(table, buffer_manager, op);
    unused_bytes -= static_cast<size_t>(size);
  }
}

inline void run_ycsb(benchmark::State& state, YCSBTable& table, YCSBOperations& operations,
                     BufferManager& buffer_manager) {
  const auto operations_per_thread = operations.size() / state.threads();
  // TODO: Reset metrics

  for (auto _ : state) {
    auto start = state.thread_index() * operations_per_thread;
    auto end = start + operations_per_thread;
    for (auto i = start; i < end; ++i) {
      const auto op = operations[i];
      execute_ycsb_action(table, buffer_manager, op);
    }
  }

  // TODO: Update state
}

}  // namespace hyrise