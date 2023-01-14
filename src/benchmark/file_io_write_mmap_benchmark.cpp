#include <fcntl.h>
#include <sys/mman.h>
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
  void mmap_write_single_threaded(benchmark::State& state, const int mmap_mode_flag, const int data_access_mode,
                                  const ssize_t file_size);
  void mmap_write_multi_threaded(benchmark::State& state, const int mmap_mode_flag, const int data_access_mode,
                                 const ssize_t file_size, uint16_t thread_count);
};

/*
 * Performs a benchmark run with the given parameters. 
 * 
 * @arguments:
 *      state: the benchmark::State object handed to the called benchmarking function.
 *      flag: The mmap flag (e.g., MAP_PRIVATE or MAP_SHARED).
 *      data_access_mode: The way the data is written.
 *      file_size: Size argument of benchmark.
*/

void FileIOWriteMmapBenchmarkFixture::mmap_write_single_threaded(benchmark::State& state, const int mmap_mode_flag,
                                                                 const int data_access_mode, const ssize_t file_size) {
  auto fd = int32_t{};
  Assert(((fd = open(filename, O_RDWR)) >= 0), close_file_and_return_error_message(fd, "Open error:", errno));

  //set output file size to avoid mapping errors
  Assert((ftruncate(fd, NUMBER_OF_BYTES) == 0), close_file_and_return_error_message(fd, "Ftruncate error:", errno));

  for (auto _ : state) {
    state.PauseTiming();
    micro_benchmark_clear_disk_cache();
    state.ResumeTiming();

    // Getting the mapping to memory.
    const auto OFFSET = off_t{0};
    auto* map = reinterpret_cast<int32_t*>(mmap(NULL, NUMBER_OF_BYTES, PROT_WRITE, mmap_mode_flag, fd, OFFSET));
    Assert(map != MAP_FAILED, "Mapping Failed:" + std::strerror(errno));

    switch (data_access_mode) {
      case SEQUENTIAL:
        memcpy(map, std::data(data_to_write), NUMBER_OF_BYTES);
        break;
      case RANDOM:
        state.PauseTiming();
        // Generating random indexes should not play a role in the benchmark.
        const auto ind_access_order = generate_random_indexes(NUMBER_OF_ELEMENTS);
        state.ResumeTiming();
        for (auto idx = uint32_t{0}; idx < ind_access_order.size(); ++idx) {
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
    if (mmap_mode_flag == MAP_PRIVATE) {
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

void write_mmap_chunk_sequential(const size_t from, const size_t to, int32_t* map, uint32_t* data_to_write_start) {
  const auto uint32_t_size = ssize_t{sizeof(uint32_t)};
  const auto bytes_to_write = static_cast<ssize_t>(uint32_t_size * (to - from));
  memcpy(map + from, data_to_write_start + from, bytes_to_write);
}

void write_mmap_chunk_random(const size_t from, const size_t to, int32_t* map,
                             const std::vector<uint32_t>& data_to_write,
                             const std::vector<uint32_t>& ind_access_order) {
  for (auto idx = from; idx < to; ++idx) {
    auto access_index = ind_access_order[idx];
    map[access_index] = data_to_write[idx];
  }
}

void FileIOWriteMmapBenchmarkFixture::mmap_write_multi_threaded(benchmark::State& state, const int mmap_mode_flag,
                                                                const int data_access_mode, const ssize_t file_size,
                                                                uint16_t thread_count) {
  auto fd = int32_t{};
  Assert(((fd = open(filename, O_RDWR)) >= 0), close_file_and_return_error_message(fd, "Open error:", errno));

  //set output file size to avoid mapping errors
  Assert((ftruncate(fd, NUMBER_OF_BYTES) == 0), close_file_and_return_error_message(fd, "Ftruncate error:", errno));

  auto threads = std::vector<std::thread>(thread_count);
  auto batch_size = static_cast<uint64_t>(std::ceil(static_cast<float>(NUMBER_OF_ELEMENTS) / thread_count));

  for (auto _ : state) {
    state.PauseTiming();
    micro_benchmark_clear_disk_cache();
    state.ResumeTiming();

    // Getting the mapping to memory.
    const auto OFFSET = off_t{0};
    auto* map = reinterpret_cast<int32_t*>(mmap(NULL, NUMBER_OF_BYTES, PROT_WRITE, mmap_mode_flag, fd, OFFSET));
    Assert(map != MAP_FAILED, "Mapping Failed:" + std::strerror(errno));

    auto* data_to_write_start = std::data(data_to_write);

    switch (data_access_mode) {
      case SEQUENTIAL:
        for (auto i = size_t{0}; i < thread_count; i++) {
          auto from = batch_size * i;
          auto to = std::min(from + batch_size, uint64_t{NUMBER_OF_ELEMENTS});
          threads[i] = std::thread(write_mmap_chunk_sequential, from, to, map, data_to_write_start);
        }
        break;
      case RANDOM:
        state.PauseTiming();
        // Generating random indexes should not play a role in the benchmark.
        const auto ind_access_order = generate_random_indexes(NUMBER_OF_ELEMENTS);
        state.ResumeTiming();

        for (auto i = size_t{0}; i < thread_count; i++) {
          auto from = batch_size * i;
          auto to = std::min(from + batch_size, uint64_t{NUMBER_OF_ELEMENTS});
          threads[i] = std::thread(write_mmap_chunk_random, from, to, map, data_to_write, ind_access_order);
        }
        break;
    }

    for (auto i = size_t{0}; i < thread_count; i++) {
      // Blocks the current thread until the thread identified by *this finishes its execution
      threads[i].join();
    }

    // After writing, sync changes to filesystem.
    Assert(msync(map, NUMBER_OF_BYTES, MS_SYNC) != -1, "Mapping Syncing Failed:" + std::strerror(errno));
    state.PauseTiming();

    // We need this because MAP_PRIVATE is copy-on-write and
    // thus written stuff is not visible in the original file.
    if (mmap_mode_flag == MAP_PRIVATE) {
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

void FileIOWriteMmapBenchmarkFixture::mmap_write_benchmark(benchmark::State& state, const int mmap_mode_flag,
                                                           const int data_access_mode, const ssize_t file_size) {
  auto thread_count = static_cast<uint16_t>(state.range(1));
  if (thread_count == 1) {
    mmap_write_single_threaded(state, mmap_mode_flag, data_access_mode, file_size);
  } else {
    mmap_write_multi_threaded(state, mmap_mode_flag, data_access_mode, file_size, thread_count);
  }
}

BENCHMARK_DEFINE_F(FileIOWriteMmapBenchmarkFixture, MMAP_ATOMIC_MAP_PRIVATE)(benchmark::State& state) {
  mmap_write_benchmark(state, MAP_PRIVATE, SEQUENTIAL, state.range(0));
}

BENCHMARK_DEFINE_F(FileIOWriteMmapBenchmarkFixture, MMAP_ATOMIC_MAP_SHARED_SEQUENTIAL)(benchmark::State& state) {
  mmap_write_benchmark(state, MAP_SHARED, SEQUENTIAL, state.range(0));
}

BENCHMARK_DEFINE_F(FileIOWriteMmapBenchmarkFixture, MMAP_ATOMIC_MAP_SHARED_RANDOM)(benchmark::State& state) {
  mmap_write_benchmark(state, MAP_SHARED, RANDOM, state.range(0));
}

// Arguments are file size in MB
BENCHMARK_REGISTER_F(FileIOWriteMmapBenchmarkFixture, MMAP_ATOMIC_MAP_PRIVATE)
    ->ArgsProduct({{10, 100, 1000}, {1, 2, 4, 8, 16, 32, 48}})
    ->UseRealTime();
BENCHMARK_REGISTER_F(FileIOWriteMmapBenchmarkFixture, MMAP_ATOMIC_MAP_SHARED_SEQUENTIAL)
    ->ArgsProduct({{10, 100, 1000}, {1, 2, 4, 8, 16, 32, 48}})
    ->UseRealTime();
BENCHMARK_REGISTER_F(FileIOWriteMmapBenchmarkFixture, MMAP_ATOMIC_MAP_SHARED_RANDOM)
    ->ArgsProduct({{10, 100, 1000}, {1, 2, 4, 8, 16, 32, 48}})
    ->UseRealTime();
}  // namespace hyrise
