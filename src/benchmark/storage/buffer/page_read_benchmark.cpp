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
  const auto num_bytes = OS_PAGE_SIZE << static_cast<size_t>(state.range(0));
  constexpr auto VIRT_SIZE = 40UL * 1024 * 1024 * 1024;
  constexpr auto FILENAME = "/home/nriek/BM_SequentialRead.bin";

  static int fd = -1;
  static std::byte* mapped_region = nullptr;

  if (state.thread_index() == 0) {
    mapped_region = mmap_region(VIRT_SIZE);
    if constexpr (SourceNode == -1) {
      explicit_move_pages(mapped_region, VIRT_SIZE, TargetNode);
      // head -c 5368709120  /dev/urandom > /home/nriek/hyrise-fork/benchmarks/BM_SequentialRead.bin
      // std::system(("head -c " + std::to_string(VIRT_SIZE) + "  /dev/urandom > " + FILENAME).c_str());
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
    }
    std::memset(mapped_region, 0x1, VIRT_SIZE);
  }

  auto page_idx = std::atomic_uint64_t{0};
  for (auto _ : state) {
    const auto iter_page_idx = page_idx.fetch_add(1);
    const auto page_ptr = mapped_region + (iter_page_idx * num_bytes);
    Assert(page_ptr < mapped_region + VIRT_SIZE, "Out of bounds");
    if constexpr (SourceNode == -1) {
      // Move SSD to CXL or DRAM
      Assert(pread(fd, page_ptr, num_bytes, iter_page_idx * num_bytes) == num_bytes, "Cannot read from file");
    } else if constexpr (SourceNode != TargetNode) {
      // Move CXL to DRAM
      explicit_move_pages(page_ptr, num_bytes, TargetNode);
    } else {
      // Noop: Stay as is, read directly
    }

    simulate_page_read(page_ptr, num_bytes);
  }

  if (state.thread_index() == 0) {
    if (fd >= 0) {
      close(fd);
    }
    //   std::filesystem::remove(FILENAME);
    unmap_region(mapped_region);
    state.SetBytesProcessed(int64_t(state.iterations()) * num_bytes);
    state.SetItemsProcessed(int64_t(state.iterations()));
  }
}

// SSD to DRAM
BENCHMARK(BM_SequentialRead<-1, 0>)
    ->ArgsProduct({benchmark::CreateDenseRange(static_cast<uint64_t>(0), static_cast<u_int64_t>(9), /*step=*/1)})
    ->DenseThreadRange(1, 48, 2)
    ->Name("BM_SequentialRead/SSDToDRAM")
    // ->Iterations(5000)
    ->UseRealTime();
BENCHMARK(BM_SequentialRead<-1, 2>)
    ->ArgsProduct({benchmark::CreateDenseRange(static_cast<uint64_t>(0), static_cast<u_int64_t>(9), /*step=*/1)})
    ->DenseThreadRange(1, 48, 2)
    ->Name("BM_SequentialRead/SSDToCXL")
    // ->Iterations(5000)
    ->UseRealTime();
BENCHMARK(BM_SequentialRead<2, 0>)
    ->ArgsProduct({benchmark::CreateDenseRange(static_cast<uint64_t>(0), static_cast<u_int64_t>(9), /*step=*/1)})
    ->DenseThreadRange(1, 48, 2)
    ->Name("BM_SequentialRead/CXLToDRAM")
    ->Iterations(5000)
    ->UseRealTime();
BENCHMARK(BM_SequentialRead<2, 2>)
    ->ArgsProduct({benchmark::CreateDenseRange(static_cast<uint64_t>(0), static_cast<u_int64_t>(9), /*step=*/1)})
    ->DenseThreadRange(1, 48, 2)
    ->Name("BM_SequentialRead/CXL")
    // ->Iterations(20000)
    ->UseRealTime();
BENCHMARK(BM_SequentialRead<0, 0>)
    ->ArgsProduct({benchmark::CreateDenseRange(static_cast<uint64_t>(0), static_cast<u_int64_t>(9), /*step=*/1)})
    ->DenseThreadRange(1, 48, 2)
    ->Name("BM_SequentialRead/DRAM")
    // ->Iterations(20000)
    ->UseRealTime();
}  // namespace hyrise