#include <fcntl.h>
#include <unistd.h>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <vector>
#include "benchmark/benchmark.h"
#include "buffer_benchmark_utils.hpp"
#include "storage/buffer/buffer_manager.hpp"

namespace hyrise {

template <int SourceNode, int TargetNode>
void BM_SequentialRead(benchmark::State& state) {
  auto mapped_region = create_mapped_region();

  const auto num_bytes = OS_PAGE_SIZE << static_cast<size_t>(state.range(0));
  constexpr auto VIRT_SIZE = 5UL * 1024 * 1024 * 1024;
  constexpr auto FILENAME = "/home/nriek/hyrise-fork/benchmarks/BM_SequentialRead.bin";
  int fd = -1;

  if constexpr (SourceNode == -1) {
    explicit_move_pages(mapped_region, VIRT_SIZE, TargetNode);
    std::system(("head -c " + std::to_string(VIRT_SIZE) + " < /dev/urandom > " + FILENAME).c_str());
#ifdef __APPLE__
    int flags = O_RDWR | O_CREAT | O_DSYNC;
#elif __linux__
    int flags = O_RDWR | O_CREAT | O_DIRECT | O_DSYNC;
#endif
    fd = open(FILENAME, flags, 0666);
    if (fd < 0) {
      Fail("Cannot open file");
    }
  } else {
    explicit_move_pages(mapped_region, VIRT_SIZE, SourceNode);
    std::memset(mapped_region, 0x1, VIRT_SIZE);
  }

  auto page_idx = std::atomic_uint64_t{0};
  for (auto _ : state) {
    const auto page_ptr = mapped_region + (++page_idx * num_bytes);
    if constexpr (SourceNode == -1) {
      // Move SSD to CXL or DRAM
      Assert(pread(fd, page_ptr, num_bytes, ++page_idx * num_bytes) == num_bytes, "Cannot read from file");
    } else if constexpr (SourceNode != TargetNode) {
      // Move CXL to DRAM
      explicit_move_pages(mapped_region, VIRT_SIZE, TargetNode);
    } else {
      // Noop: Stay as is, read directly
    }

    simulate_page_read(page_ptr, num_bytes);
  }

  state.SetItemsProcessed(int64_t(state.iterations()));
  state.SetBytesProcessed(int64_t(state.iterations()) * num_bytes);

  close(fd);
  std::filesystem::remove(FILENAME);
  unmap_region(mapped_region);
}

// SSD to DRAM
BENCHMARK(BM_SequentialRead<-1, 0>)
    ->ArgsProduct({benchmark::CreateDenseRange(static_cast<uint64_t>(0), static_cast<u_int64_t>(9), /*step=*/1)})
    ->ThreadRange(1, 48)
    ->Name("SSDToDRAM");
BENCHMARK(BM_SequentialRead<-1, 2>)
    ->ArgsProduct({benchmark::CreateDenseRange(static_cast<uint64_t>(0), static_cast<u_int64_t>(9), /*step=*/1)})
    ->ThreadRange(1, 48)
    ->Name("SSDToCXL");
BENCHMARK(BM_SequentialRead<2, 0>)
    ->ArgsProduct({benchmark::CreateDenseRange(static_cast<uint64_t>(0), static_cast<u_int64_t>(9), /*step=*/1)})
    ->ThreadRange(1, 48)
    ->Name("CXLToDram");
BENCHMARK(BM_SequentialRead<2, 2>)
    ->ArgsProduct({benchmark::CreateDenseRange(static_cast<uint64_t>(0), static_cast<u_int64_t>(9), /*step=*/1)})
    ->ThreadRange(1, 48)
    ->Name("CXL");
BENCHMARK(BM_SequentialRead<0, 0>)
    ->ArgsProduct({benchmark::CreateDenseRange(static_cast<uint64_t>(0), static_cast<u_int64_t>(9), /*step=*/1)})
    ->ThreadRange(1, 48)
    ->Name("DRAM");
}  // namespace hyrise