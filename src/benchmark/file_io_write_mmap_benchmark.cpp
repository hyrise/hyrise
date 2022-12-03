#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <algorithm>
#include <numeric>

#include "file_io_write_micro_benchmark.hpp"

namespace hyrise {

class FileIOWriteMmapBenchmarkFixture : public FileIOWriteMicroBenchmarkFixture {
 protected:
  void mmap_write_benchmark(benchmark::State& state, const int flag, const int data_access_mode,
                            const ssize_t file_size);
};

/*
 * Performs a benchmark run with the given parameters. 
 * 
 * @arguments:
 *      state: the benchmark::State object handed to the called benchmarking function.
 *      flag: The mmap flag (e.g., MAP_PRIVATE or MAP_SHARED).
 *      data_access_mode: The way the data is written.
 *                  (-1)  No data access
 *                  (0)   Sequential
 *                  (1)   Random
 *      file_size: Size argument of benchmark.
*/
void FileIOWriteMmapBenchmarkFixture::mmap_write_benchmark(benchmark::State& state, const int flag,
                                                           const int data_access_mode, const ssize_t file_size) {
  auto fd = int32_t{};
  if ((fd = open(filename, O_RDWR)) < 0) {
    close(fd);
    Fail("Open error:" + std::strerror(errno));
  }

  // set output file size to avoid mapping errors
  if (ftruncate(fd, NUMBER_OF_BYTES) < 0) {
    close(fd);
    Fail("Ftruncate error:" + std::strerror(errno));
  }

  for (auto _ : state) {
    state.PauseTiming();
    micro_benchmark_clear_disk_cache();
    state.ResumeTiming();

    // Getting the mapping to memory.
    const auto OFFSET = off_t{0};
    int32_t* map = reinterpret_cast<int32_t*>(mmap(NULL, NUMBER_OF_BYTES, PROT_WRITE, flag, fd, OFFSET));
    Assert(map != MAP_FAILED, "Mapping Failed:" + std::strerror(errno));

    switch (data_access_mode) {
      case 0:
        memcpy(map, std::data(data_to_write), NUMBER_OF_BYTES);
        break;
      case 1:
        state.PauseTiming();
        // Generating random indexes should not play a role in the benchmark.
        const auto ind_access_order = generate_random_indexes(NUMBER_OF_ELEMENTS);
        state.ResumeTiming();
        for (uint32_t idx = 0; idx < ind_access_order.size(); ++idx) {
          auto access_index = ind_access_order[idx];
          map[access_index] = data_to_write[idx];
        }
        break;
    }

    // After writing, sync changes to filesystem.
    Assert(msync(map, NUMBER_OF_BYTES, MS_SYNC) != -1, "Mapping Syncing Failed:" + std::strerror(errno));
    state.PauseTiming();

    // We need this because MAP_PRIVATE is copy-on-write and
    // thus written stuff is not visible in the original file.
    if (flag == MAP_PRIVATE) {
      std::vector<uint32_t> read_data;
      read_data.resize(NUMBER_OF_ELEMENTS);
      memcpy(std::data(read_data), map, NUMBER_OF_BYTES);
      auto sum = std::accumulate(read_data.begin(), read_data.end(), uint64_t{0});
      Assert(control_sum == sum,
             "Sanity check failed. Got: " + std::to_string(sum) + "Expected: " + std::to_string(control_sum));
    } else {
      sanity_check();
    }

    state.ResumeTiming();

    // Remove memory mapping after job is done.
    Assert(munmap(map, NUMBER_OF_BYTES) == 0, "Unmapping failed:" + std::strerror(errno));
  }

  close(fd);
}

BENCHMARK_DEFINE_F(FileIOWriteMmapBenchmarkFixture, MMAP_ATOMIC_MAP_PRIVATE)(benchmark::State& state) {
  mmap_write_benchmark(state, MAP_PRIVATE, 0, state.range(0));
}

BENCHMARK_DEFINE_F(FileIOWriteMmapBenchmarkFixture, MMAP_ATOMIC_MAP_SHARED_SEQUENTIAL)(benchmark::State& state) {
  mmap_write_benchmark(state, MAP_SHARED, 0, state.range(0));
}

BENCHMARK_DEFINE_F(FileIOWriteMmapBenchmarkFixture, MMAP_ATOMIC_MAP_SHARED_RANDOM)(benchmark::State& state) {
  mmap_write_benchmark(state, MAP_SHARED, 1, state.range(0));
}

// Arguments are file size in MB
BENCHMARK_REGISTER_F(FileIOWriteMmapBenchmarkFixture, MMAP_ATOMIC_MAP_PRIVATE)->Arg(10)->Arg(100)->Arg(1000);
BENCHMARK_REGISTER_F(FileIOWriteMmapBenchmarkFixture, MMAP_ATOMIC_MAP_SHARED_SEQUENTIAL)->Arg(10)->Arg(100)->Arg(1000);
BENCHMARK_REGISTER_F(FileIOWriteMmapBenchmarkFixture, MMAP_ATOMIC_MAP_SHARED_RANDOM)->Arg(10)->Arg(100)->Arg(1000);

}  // namespace hyrise
