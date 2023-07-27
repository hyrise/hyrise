#include <fcntl.h>
#include <unistd.h>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <vector>
#include "benchmark/benchmark.h"
#include "buffer_benchmark_utils.hpp"

namespace hyrise {
enum AccessType { Read, Write };

template <int node, AccessType access>
void BM_RandomAccess(benchmark::State& state) {
  const auto num_bytes = CACHE_LINE_SIZE << static_cast<size_t>(state.range(0));
  constexpr auto VIRT_SIZE = 30UL * GB;
  constexpr auto FILENAME = "/home/nriek/BM_SequentialRead.bin";
  auto max_index = VIRT_SIZE / num_bytes;

  static int fd = -1;
  static std::byte* mapped_region = nullptr;

  if (state.thread_index() == 0) {
    mapped_region = mmap_region(VIRT_SIZE);
    if constexpr (node == -1) {
      explicit_move_pages(mapped_region, VIRT_SIZE, 0);
      fd = open_file(FILENAME);
    } else {
      explicit_move_pages(mapped_region, VIRT_SIZE, node);
    }
    std::memset(mapped_region, 0x1, VIRT_SIZE);
  }

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> distribution(0, max_index);

  for (auto _ : state) {
    auto curr_idx = distribution(gen);

    if constexpr (node == -1) {
      if constexpr (access == Read) {
        // Move SSD to CXL or DRAM
        Assert(pread(fd, mapped_region + curr_idx, VIRT_SIZE, 0) == VIRT_SIZE, "Cannot read from file");
      } else {
        // Move DRAM to SSD
        Assert(pwrite(fd, mapped_region + curr_idx, VIRT_SIZE, 0) == VIRT_SIZE, "Cannot write to file");
      }
    } else {
      if constexpr (access == Read) {
        simulate_read(mapped_region + curr_idx, VIRT_SIZE);
      } else {
        simulate_store(mapped_region + curr_idx, VIRT_SIZE);
      }
    }
  }

  if (state.thread_index() == 0) {
    if (fd >= 0) {
      close(fd);
    }
    //   std::filesystem::remove(FILENAME);
    munmap_region(mapped_region, VIRT_SIZE);
    state.SetItemsProcessed(int64_t(state.iterations()) * state.threads());
    state.SetBytesProcessed(int64_t(state.iterations()) * num_bytes * state.threads());
  }

  for (auto _ : state) {}
}

BENCHMARK(BM_RandomAccess<0, AccessType::Read>)
    ->ArgsProduct({benchmark::CreateDenseRange(static_cast<uint64_t>(0), static_cast<u_int64_t>(9), /*step=*/1)})
    ->DenseThreadRange(1, 48, 2)
    ->Name("BM_RandomRead/DRAM")
    ->UseRealTime();

BENCHMARK(BM_RandomAccess<0, AccessType::Write>)
    ->ArgsProduct({benchmark::CreateDenseRange(static_cast<uint64_t>(0), static_cast<u_int64_t>(9), /*step=*/1)})
    ->DenseThreadRange(1, 48, 2)
    ->Name("BM_RandomWrite/DRAM")
    ->UseRealTime();

BENCHMARK(BM_RandomAccess<-1, AccessType::Read>)
    ->ArgsProduct({benchmark::CreateDenseRange(static_cast<uint64_t>(0), static_cast<u_int64_t>(9), /*step=*/1)})
    ->DenseThreadRange(1, 48, 2)
    ->Name("BM_RandomRead/SSD")
    ->UseRealTime();

BENCHMARK(BM_RandomAccess<-1, AccessType::Write>)
    ->ArgsProduct({benchmark::CreateDenseRange(static_cast<uint64_t>(0), static_cast<u_int64_t>(9), /*step=*/1)})
    ->DenseThreadRange(1, 48, 2)
    ->Name("BM_RandomWrite/SSD")
    ->UseRealTime();

BENCHMARK(BM_RandomAccess<2, AccessType::Read>)
    ->ArgsProduct({benchmark::CreateDenseRange(static_cast<uint64_t>(0), static_cast<u_int64_t>(9), /*step=*/1)})
    ->DenseThreadRange(1, 48, 2)
    ->Name("BM_RandomRead/CXL")
    ->UseRealTime();

BENCHMARK(BM_RandomAccess<2, AccessType::Write>)
    ->ArgsProduct({benchmark::CreateDenseRange(static_cast<uint64_t>(0), static_cast<u_int64_t>(9), /*step=*/1)})
    ->DenseThreadRange(1, 48, 2)
    ->Name("BM_RandomWrite/CXL")
    ->UseRealTime();

}  // namespace hyrise