#include "file_io_read_micro_benchmark.hpp"

#include <umap/umap.h>

namespace {

// Worker function for threading.
void read_mmap_chunk_sequential(const size_t from, const size_t to, const int32_t* map, uint64_t& sum) {
  for (auto index = size_t{0} + from; index < to; ++index) {
    sum += map[index];
  }
}

// Worker function for threading.
void read_mmap_chunk_random(const size_t from, const size_t to, const int32_t* map, uint64_t& sum,
                            const std::vector<uint32_t>& random_indexes) {
  for (auto index = size_t{0} + from; index < to; ++index) {
    sum += map[random_indexes[index]];
  }
}

}  // namespace

namespace hyrise {

void FileIOMicroReadBenchmarkFixture::memory_mapped_read_single_threaded(benchmark::State& state, const int mapping_type, const int map_mode_flag, const int access_order) {
  auto fd = int32_t{};
  Assert(((fd = open(filename, O_RDONLY)) >= 0), fail_and_close_file(fd, "Open error: ", errno));

  for (auto _ : state) {
    state.PauseTiming();
    micro_benchmark_clear_disk_cache();
    state.ResumeTiming();

    // Getting the mapping to memory.
    const auto OFFSET = off_t{0};

    auto* map = reinterpret_cast<int32_t*>(mmap(NULL, NUMBER_OF_BYTES, PROT_READ, map_mode_flag, fd, OFFSET));

    if (mapping_type == MMAP) {
      map = reinterpret_cast<int32_t*>(mmap(NULL, NUMBER_OF_BYTES, PROT_READ, map_mode_flag, fd, OFFSET));
    } else if (mapping_type == UMAP) {
      map = reinterpret_cast<int32_t*>(umap(NULL, NUMBER_OF_BYTES, PROT_READ, map_mode_flag, fd, OFFSET));
    } else {
      Fail("Error: Invalid mapping type.");
    }

    Assert((map != MAP_FAILED), fail_and_close_file(fd, "Mapping Failed: ", errno));

    auto sum = uint64_t{0};
    if (access_order == RANDOM) {
        madvise(map, NUMBER_OF_BYTES, MADV_RANDOM);
        state.PauseTiming();
        const auto random_indexes = generate_random_indexes(NUMBER_OF_ELEMENTS);
        state.ResumeTiming();
        for (auto index = size_t{0}; index < NUMBER_OF_ELEMENTS; ++index) {
          sum += map[random_indexes[index]];
        }
    } else /* if (access_order == SEQUENTIAL) */ {
        madvise(map, NUMBER_OF_BYTES, MADV_SEQUENTIAL);
        for (auto index = size_t{0}; index < NUMBER_OF_ELEMENTS; ++index) {
          sum += map[index];
        }
    }

    state.PauseTiming();
    Assert(control_sum == sum, "Sanity check failed: Not the same result");
    state.ResumeTiming();

    // Remove memory mapping after job is done.
    if (mapping_type == MMAP) {
      Assert((munmap(map, NUMBER_OF_BYTES) == 0), fail_and_close_file(fd, "Unmapping failed: ", errno));
    } else /* if (mapping_type == UMAP) */ {
      Assert((uunmap(map, NUMBER_OF_BYTES) == 0), fail_and_close_file(fd, "Unmapping failed: ", errno));
    } 
  }

  close(fd);
}

void FileIOMicroReadBenchmarkFixture::memory_mapped_read_multi_threaded(benchmark::State& state, const int mapping_type, const int map_mode_flag, const uint16_t thread_count, const int access_order) {
  auto fd = int32_t{};
  Assert(((fd = open(filename, O_RDONLY)) >= 0), fail_and_close_file(fd, "Open error: ", errno));

  auto threads = std::vector<std::thread>(thread_count);
  const auto batch_size = static_cast<uint64_t>(std::ceil(static_cast<float>(NUMBER_OF_ELEMENTS) / thread_count));

  for (auto _ : state) {
    state.PauseTiming();
    micro_benchmark_clear_disk_cache();
    auto sums = std::vector<uint64_t>(thread_count);
    state.ResumeTiming();

    // Getting the mapping to memory.
    const auto OFFSET = off_t{0};

    auto* map = reinterpret_cast<int32_t*>(mmap(NULL, NUMBER_OF_BYTES, PROT_READ, map_mode_flag, fd, OFFSET));

    if (mapping_type == MMAP) {
      map = reinterpret_cast<int32_t*>(mmap(NULL, NUMBER_OF_BYTES, PROT_READ, map_mode_flag, fd, OFFSET));
    } else if (mapping_type == UMAP) {
      map = reinterpret_cast<int32_t*>(umap(NULL, NUMBER_OF_BYTES, PROT_READ, map_mode_flag, fd, OFFSET));
    } else {
      Fail("Error: Invalid mapping type.");
    }

    Assert((map != MAP_FAILED), fail_and_close_file(fd, "Mapping Failed: ", errno));

    if (access_order == RANDOM) {
      state.PauseTiming();
      const auto random_indexes = generate_random_indexes(NUMBER_OF_ELEMENTS);
      state.ResumeTiming();

      if (mapping_type == MMAP) {
        madvise(map, NUMBER_OF_BYTES, MADV_RANDOM);
      }

      for (auto i = size_t{0}; i < thread_count; ++i) {
        const auto from = batch_size * i;
        const auto to = std::min(from + batch_size, uint64_t{NUMBER_OF_ELEMENTS});
        // std::ref fix from https://stackoverflow.com/a/73642536
        threads[i] = std::thread(read_mmap_chunk_random, from, to, map, std::ref(sums[i]), random_indexes);
      }
    } else {
      if (mapping_type == MMAP) {
        madvise(map, NUMBER_OF_BYTES, MADV_SEQUENTIAL);
      }

      for (auto i = size_t{0}; i < thread_count; ++i) {
        const auto from = batch_size * i;
        const auto to = std::min(from + batch_size, uint64_t{NUMBER_OF_ELEMENTS});
        // std::ref fix from https://stackoverflow.com/a/73642536
        threads[i] = std::thread(read_mmap_chunk_sequential, from, to, map, std::ref(sums[i]));
      }
    }
    for (auto i = size_t{0}; i < thread_count; ++i) {
      // Blocks the current thread until the thread identified by *this finishes its execution
      threads[i].join();
    }
    state.PauseTiming();
    const auto total_sum = std::accumulate(sums.begin(), sums.end(), uint64_t{0});

    Assert(control_sum == total_sum, "Sanity check failed: Not the same result");
    state.ResumeTiming();

    if (mapping_type == MMAP) {
      Assert(msync(map, NUMBER_OF_BYTES, MS_SYNC) != -1, "Mapping Syncing Failed:" + std::strerror(errno));
    }

    state.PauseTiming();

    // Remove memory mapping after job is done.
    if (mapping_type == MMAP) {
      Assert((munmap(map, NUMBER_OF_BYTES) == 0), fail_and_close_file(fd, "Unmapping failed: ", errno));
    } else /* if (mapping_type == UMAP) */ {
      Assert((uunmap(map, NUMBER_OF_BYTES) == 0), fail_and_close_file(fd, "Unmapping failed: ", errno));
    } 
    state.ResumeTiming();
  }

  close(fd);
}

BENCHMARK_DEFINE_F(FileIOMicroReadBenchmarkFixture, MMAP_ATOMIC_MAP_PRIVATE_RANDOM)(benchmark::State& state) {
  const auto thread_count = static_cast<uint16_t>(state.range(1));
  if (thread_count == 1) {
    memory_mapped_read_single_threaded(state, MMAP, PRIVATE, RANDOM);
  } else {
    memory_mapped_read_multi_threaded(state, MMAP, PRIVATE, thread_count, RANDOM);
  }
}

BENCHMARK_DEFINE_F(FileIOMicroReadBenchmarkFixture, MMAP_ATOMIC_MAP_PRIVATE_SEQUENTIAL)(benchmark::State& state) {
  const auto thread_count = static_cast<uint16_t>(state.range(1));
  if (thread_count == 1) {
    memory_mapped_read_single_threaded(state, MMAP, PRIVATE, SEQUENTIAL);
  } else {
    memory_mapped_read_multi_threaded(state, MMAP, PRIVATE, thread_count, SEQUENTIAL);
  }
}

BENCHMARK_DEFINE_F(FileIOMicroReadBenchmarkFixture, MMAP_ATOMIC_MAP_SHARED_RANDOM)(benchmark::State& state) {
  const auto thread_count = static_cast<uint16_t>(state.range(1));
  if (thread_count == 1) {
    memory_mapped_read_single_threaded(state, MMAP, SHARED, RANDOM);
  } else {
    memory_mapped_read_multi_threaded(state, MMAP, SHARED, thread_count, RANDOM);
  }
}

BENCHMARK_DEFINE_F(FileIOMicroReadBenchmarkFixture, MMAP_ATOMIC_MAP_SHARED_SEQUENTIAL)(benchmark::State& state) {
  const auto thread_count = static_cast<uint16_t>(state.range(1));
  if (thread_count == 1) {
    memory_mapped_read_single_threaded(state, MMAP, SHARED, SEQUENTIAL);
  } else {
    memory_mapped_read_multi_threaded(state, MMAP, SHARED, thread_count, SEQUENTIAL);
  }
}

BENCHMARK_DEFINE_F(FileIOMicroReadBenchmarkFixture, UMAP_ATOMIC_MAP_PRIVATE_RANDOM)(benchmark::State& state) {
  const auto thread_count = static_cast<uint16_t>(state.range(1));
  if (thread_count == 1) {
    memory_mapped_read_single_threaded(state, UMAP, PRIVATE, RANDOM);
  } else {
    memory_mapped_read_multi_threaded(state, UMAP, PRIVATE, thread_count, RANDOM);
  }
}

BENCHMARK_DEFINE_F(FileIOMicroReadBenchmarkFixture, UMAP_ATOMIC_MAP_PRIVATE_SEQUENTIAL)(benchmark::State& state) {
  const auto thread_count = static_cast<uint16_t>(state.range(1));
  if (thread_count == 1) {
    memory_mapped_read_single_threaded(state, UMAP, PRIVATE, SEQUENTIAL);
  } else {
    memory_mapped_read_multi_threaded(state, UMAP, PRIVATE, thread_count, SEQUENTIAL);
  }
}

BENCHMARK_REGISTER_F(FileIOMicroReadBenchmarkFixture, MMAP_ATOMIC_MAP_PRIVATE_SEQUENTIAL)
    ->ArgsProduct({{10, 100, 1000}, {1, 2, 4, 8, 16, 24, 32, 48}})
    ->UseRealTime();
BENCHMARK_REGISTER_F(FileIOMicroReadBenchmarkFixture, MMAP_ATOMIC_MAP_PRIVATE_RANDOM)
    ->ArgsProduct({{10, 100, 1000}, {1, 2, 4, 8, 16, 24, 32, 48}})
    ->UseRealTime();

BENCHMARK_REGISTER_F(FileIOMicroReadBenchmarkFixture, MMAP_ATOMIC_MAP_SHARED_SEQUENTIAL)
    ->ArgsProduct({{10, 100, 1000}, {1, 2, 4, 8, 16, 24, 32, 48}})
    ->UseRealTime();
BENCHMARK_REGISTER_F(FileIOMicroReadBenchmarkFixture, MMAP_ATOMIC_MAP_SHARED_RANDOM)
    ->ArgsProduct({{10, 100, 1000}, {1, 2, 4, 8, 16, 24, 32, 48}})
    ->UseRealTime();

BENCHMARK_REGISTER_F(FileIOMicroReadBenchmarkFixture, UMAP_ATOMIC_MAP_PRIVATE_SEQUENTIAL)
    ->ArgsProduct({{100}, {1, 2, 4, 8, 16, 32}})
    ->UseRealTime();

BENCHMARK_REGISTER_F(FileIOMicroReadBenchmarkFixture, UMAP_ATOMIC_MAP_PRIVATE_RANDOM)
    ->ArgsProduct({{100}, {1, 2, 4, 8, 16, 32}})
    ->UseRealTime();

}  // namespace hyrise
