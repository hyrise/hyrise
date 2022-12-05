#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <iterator>
#include <numeric>
#include <random>
#include <thread>
#include "file_io_read_micro_benchmark.hpp"
#include "micro_benchmark_basic_fixture.hpp"

namespace hyrise {

void read_data_using_read(const size_t from, const size_t to, int32_t fd, uint32_t* read_data_start) {
  const auto uint32_t_size = ssize_t{sizeof(uint32_t)};
  const auto bytes_to_read = static_cast<ssize_t>(uint32_t_size * (to - from));
  lseek(fd, from * uint32_t_size, SEEK_SET);
  Assert((read(fd, read_data_start + from, bytes_to_read) == bytes_to_read),
         fail_and_close_file(fd, "Read error: ", errno));
}

void read_data_randomly_using_read(const size_t from, const size_t to, int32_t fd, uint32_t* read_data_start,
                                   const std::vector<uint32_t>& random_indices) {
  const auto uint32_t_size = ssize_t{sizeof(uint32_t)};

  lseek(fd, 0, SEEK_SET);
  // TODO(everyone): Randomize inidzes to not read all the data but really randomize the reads to read same amount but
  //  incl possible duplicates
  for (auto index = from; index < to; ++index) {
    lseek(fd, uint32_t_size * random_indices[index], SEEK_SET);
    Assert((read(fd, read_data_start + index, uint32_t_size) == uint32_t_size),
           fail_and_close_file(fd, "Read error: ", errno));
  }
}

void read_data_using_pread(const size_t from, const size_t to, int32_t fd, uint32_t* read_data_start) {
  const auto uint32_t_size = ssize_t{sizeof(uint32_t)};
  const auto bytes_to_read = static_cast<ssize_t>(uint32_t_size * (to - from));
  lseek(fd, from * uint32_t_size, SEEK_SET);
  Assert((pread(fd, read_data_start + from, bytes_to_read, from * uint32_t_size) == bytes_to_read),
         fail_and_close_file(fd, "Read error: ", errno));
}

void read_data_randomly_using_pread(const size_t from, const size_t to, int32_t fd, uint32_t* read_data_start,
                                    const std::vector<uint32_t>& random_indices) {
  const auto uint32_t_size = ssize_t{sizeof(uint32_t)};

  lseek(fd, 0, SEEK_SET);
  // TODO(everyone): Randomize inidzes to not read all the data but really randomize the reads to read same amount but
  //  incl possible duplicates
  for (auto index = from; index < to; ++index) {
    Assert((pread(fd, read_data_start + index, uint32_t_size, uint32_t_size * random_indices[index]) == uint32_t_size),
           fail_and_close_file(fd, "Read error: ", errno));
  }
}

// TODO(everyone): Reduce LOC by making this function more modular (do not repeat it with different function inputs).
void FileIOMicroReadBenchmarkFixture::read_non_atomic_multi_threaded(benchmark::State& state, uint16_t thread_count) {
  auto filedescriptors = std::vector<int32_t>(thread_count);
  for (auto i = size_t{0}; i < thread_count; i++) {
    auto fd = int32_t{};
    Assert(((fd = open(filename, O_RDONLY)) >= 0), fail_and_close_file(fd, "Open error: ", errno));
    filedescriptors[i] = fd;
  }

  auto threads = std::vector<std::thread>(thread_count);
  auto batch_size = static_cast<uint64_t>(std::ceil(static_cast<float>(NUMBER_OF_ELEMENTS) / thread_count));

  for (auto _ : state) {
    state.PauseTiming();

    micro_benchmark_clear_disk_cache();
    auto read_data = std::vector<uint32_t>{};
    read_data.resize(NUMBER_OF_ELEMENTS);
    auto* read_data_start = std::data(read_data);

    state.ResumeTiming();

    for (auto i = size_t{0}; i < thread_count; i++) {
      auto from = batch_size * i;
      auto to = from + batch_size;
      if (to >= NUMBER_OF_ELEMENTS) {
        to = NUMBER_OF_ELEMENTS;
      }
      threads[i] = (std::thread(read_data_using_read, from, to, filedescriptors[i], read_data_start));
    }

    for (auto i = size_t{0}; i < thread_count; i++) {
      // Explain: Blocks the current thread until the thread identified by *this finishes its execution
      threads[i].join();
    }
    state.PauseTiming();

    const auto sum = std::accumulate(read_data.begin(), read_data.end(), uint64_t{0});
    Assert(control_sum == sum, "Sanity check failed: Not the same result");
    state.ResumeTiming();
  }

  for (auto i = size_t{0}; i < thread_count; i++) {
    close(filedescriptors[i]);
  }
}

void FileIOMicroReadBenchmarkFixture::read_non_atomic_single_threaded(benchmark::State& state) {
  auto fd = int32_t{};
  Assert(((fd = open(filename, O_RDONLY)) >= 0), fail_and_close_file(fd, "Open error: ", errno));

  for (auto _ : state) {
    state.PauseTiming();

    micro_benchmark_clear_disk_cache();
    auto read_data = std::vector<uint32_t>{};
    read_data.resize(NUMBER_OF_ELEMENTS);

    state.ResumeTiming();

    lseek(fd, 0, SEEK_SET);
    Assert((read(fd, std::data(read_data), NUMBER_OF_BYTES) == NUMBER_OF_BYTES),
           fail_and_close_file(fd, "Read error: ", errno));

    state.PauseTiming();

    const auto sum = std::accumulate(read_data.begin(), read_data.end(), uint64_t{0});
    Assert(control_sum == sum, "Sanity check failed: Not the same result");

    state.ResumeTiming();
  }

  close(fd);
}

void FileIOMicroReadBenchmarkFixture::read_non_atomic_random_single_threaded(benchmark::State& state) {
  auto fd = int32_t{};
  Assert(((fd = open(filename, O_RDONLY)) >= 0), fail_and_close_file(fd, "Open error: ", errno));

  for (auto _ : state) {
    state.PauseTiming();

    micro_benchmark_clear_disk_cache();
    const auto random_indices = generate_random_indexes(NUMBER_OF_ELEMENTS);
    auto read_data = std::vector<uint32_t>{};
    read_data.resize(NUMBER_OF_ELEMENTS);

    state.ResumeTiming();

    lseek(fd, 0, SEEK_SET);
    // TODO(everyone): Randomize inidzes to not read all the data but really randomize the reads to read same amount but
    //  incl possible duplicates
    for (auto index = size_t{0}; index < NUMBER_OF_ELEMENTS; ++index) {
      lseek(fd, uint32_t_size * random_indices[index], SEEK_SET);
      Assert((read(fd, std::data(read_data) + index, uint32_t_size) == uint32_t_size),
             fail_and_close_file(fd, "Read error: ", errno));
    }

    state.PauseTiming();

    const auto sum = std::accumulate(read_data.begin(), read_data.end(), uint64_t{0});
    Assert(control_sum == sum, "Sanity check failed: Not the same result");

    state.ResumeTiming();
  }

  close(fd);
}

void FileIOMicroReadBenchmarkFixture::read_non_atomic_random_multi_threaded(benchmark::State& state,
                                                                            uint16_t thread_count) {
  auto filedescriptors = std::vector<int32_t>(thread_count);
  for (auto i = size_t{0}; i < thread_count; i++) {
    auto fd = int32_t{};
    Assert(((fd = open(filename, O_RDONLY)) >= 0), fail_and_close_file(fd, "Open error: ", errno));
    filedescriptors[i] = fd;
  }

  auto threads = std::vector<std::thread>(thread_count);
  auto batch_size = static_cast<uint64_t>(std::ceil(static_cast<float>(NUMBER_OF_ELEMENTS) / thread_count));

  for (auto _ : state) {
    state.PauseTiming();

    micro_benchmark_clear_disk_cache();
    const auto random_indices = generate_random_indexes(NUMBER_OF_ELEMENTS);
    auto read_data = std::vector<uint32_t>{};
    read_data.resize(NUMBER_OF_ELEMENTS);

    state.ResumeTiming();
    for (auto i = size_t{0}; i < thread_count; i++) {
      auto from = batch_size * i;
      auto to = from + batch_size;
      if (to >= NUMBER_OF_ELEMENTS) {
        to = NUMBER_OF_ELEMENTS;
      }
      threads[i] = (std::thread(read_data_randomly_using_read, from, to, filedescriptors[i], std::data(read_data),
                                random_indices));
    }

    for (auto i = size_t{0}; i < thread_count; i++) {
      // Explain: Blocks the current thread until the thread identified by *this finishes its execution
      threads[i].join();
    }
    state.PauseTiming();

    const auto sum = std::accumulate(read_data.begin(), read_data.end(), uint64_t{0});
    Assert(control_sum == sum, "Sanity check failed: Not the same result");

    state.ResumeTiming();
  }

  for (auto i = size_t{0}; i < thread_count; i++) {
    close(filedescriptors[i]);
  }
}

void FileIOMicroReadBenchmarkFixture::pread_atomic_single_threaded(benchmark::State& state) {
  auto fd = int32_t{};
  Assert(((fd = open(filename, O_RDONLY)) >= 0), fail_and_close_file(fd, "Open error: ", errno));

  for (auto _ : state) {
    state.PauseTiming();

    micro_benchmark_clear_disk_cache();
    auto read_data = std::vector<uint32_t>{};
    read_data.resize(NUMBER_OF_ELEMENTS);
    state.ResumeTiming();

    read_data.resize(NUMBER_OF_ELEMENTS);

    state.ResumeTiming();

    Assert((pread(fd, std::data(read_data), NUMBER_OF_BYTES, 0) == NUMBER_OF_BYTES),
           fail_and_close_file(fd, "Read error: ", errno));

    state.PauseTiming();

    const auto sum = std::accumulate(read_data.begin(), read_data.end(), uint64_t{0});
    Assert(control_sum == sum, "Sanity check failed: Not the same result");
    state.ResumeTiming();
  }

  close(fd);
}

void FileIOMicroReadBenchmarkFixture::pread_atomic_multi_threaded(benchmark::State& state, uint16_t thread_count) {
  auto fd = int32_t{};
  Assert(((fd = open(filename, O_RDONLY)) >= 0), fail_and_close_file(fd, "Open error: ", errno));

  auto threads = std::vector<std::thread>(thread_count);
  auto batch_size = static_cast<uint64_t>(std::ceil(static_cast<float>(NUMBER_OF_ELEMENTS) / thread_count));

  for (auto _ : state) {
    state.PauseTiming();

    micro_benchmark_clear_disk_cache();
    auto read_data = std::vector<uint32_t>{};
    read_data.resize(NUMBER_OF_ELEMENTS);
    auto* read_data_start = std::data(read_data);

    state.ResumeTiming();

    for (auto i = size_t{0}; i < thread_count; i++) {
      auto from = batch_size * i;
      auto to = from + batch_size;
      if (to >= NUMBER_OF_ELEMENTS) {
        to = NUMBER_OF_ELEMENTS;
      }
      threads[i] = (std::thread(read_data_using_pread, from, to, fd, read_data_start));
    }

    for (auto i = size_t{0}; i < thread_count; i++) {
      // Explain: Blocks the current thread until the thread identified by *this finishes its execution
      threads[i].join();
    }
    state.PauseTiming();

    const auto sum = std::accumulate(read_data.begin(), read_data.end(), uint64_t{0});
    Assert(control_sum == sum, "Sanity check failed: Not the same result");
    state.ResumeTiming();
  }

  close(fd);
}

void FileIOMicroReadBenchmarkFixture::pread_atomic_random_single_threaded(benchmark::State& state) {
  auto fd = int32_t{};
  Assert(((fd = open(filename, O_RDONLY)) >= 0), fail_and_close_file(fd, "Open error: ", errno));

  for (auto _ : state) {
    state.PauseTiming();
    micro_benchmark_clear_disk_cache();
    const auto random_indices = generate_random_indexes(NUMBER_OF_ELEMENTS);
    auto read_data = std::vector<uint32_t>{};
    read_data.resize(NUMBER_OF_ELEMENTS);

    state.ResumeTiming();

    // TODO(everyone) Randomize inidzes to not read all the data but really randomize
    for (auto index = size_t{0}; index < NUMBER_OF_ELEMENTS; ++index) {
      Assert((pread(fd, std::data(read_data) + index, uint32_t_size, uint32_t_size * random_indices[index]) ==
              uint32_t_size),
             fail_and_close_file(fd, "Read error: ", errno));
    }

    state.PauseTiming();

    const auto sum = std::accumulate(read_data.begin(), read_data.end(), uint64_t{0});
    Assert(control_sum == sum, "Sanity check failed: Not the same result");

    state.ResumeTiming();
  }

  close(fd);
}

void FileIOMicroReadBenchmarkFixture::pread_atomic_random_multi_threaded(benchmark::State& state,
                                                                             uint16_t thread_count) {
  auto fd = int32_t{};
  Assert(((fd = open(filename, O_RDONLY)) >= 0), fail_and_close_file(fd, "Open error: ", errno));

  auto threads = std::vector<std::thread>(thread_count);
  auto batch_size = static_cast<uint64_t>(std::ceil(static_cast<float>(NUMBER_OF_ELEMENTS) / thread_count));

  for (auto _ : state) {
    state.PauseTiming();

    micro_benchmark_clear_disk_cache();
    const auto random_indices = generate_random_indexes(NUMBER_OF_ELEMENTS);
    auto read_data = std::vector<uint32_t>{};
    read_data.resize(NUMBER_OF_ELEMENTS);

    state.ResumeTiming();
    for (auto i = size_t{0}; i < thread_count; i++) {
      auto from = batch_size * i;
      auto to = from + batch_size;
      if (to >= NUMBER_OF_ELEMENTS) {
        to = NUMBER_OF_ELEMENTS;
      }
      threads[i] = (std::thread(read_data_randomly_using_pread, from, to, fd, std::data(read_data),
                                random_indices));
    }

    for (auto i = size_t{0}; i < thread_count; i++) {
      // Explain: Blocks the current thread until the thread identified by *this finishes its execution
      threads[i].join();
    }
    state.PauseTiming();

    const auto sum = std::accumulate(read_data.begin(), read_data.end(), uint64_t{0});
    Assert(control_sum == sum, "Sanity check failed: Not the same result");

    state.ResumeTiming();
  }

  close(fd);
}

BENCHMARK_DEFINE_F(FileIOMicroReadBenchmarkFixture, READ_NON_ATOMIC_SEQUENTIAL_THREADED)(benchmark::State& state) {
  auto thread_count = static_cast<uint16_t>(state.range(1));
  if (thread_count == 1) {
    read_non_atomic_single_threaded(state);
  } else {
    read_non_atomic_multi_threaded(state, thread_count);
  }
}

BENCHMARK_DEFINE_F(FileIOMicroReadBenchmarkFixture, READ_NON_ATOMIC_RANDOM_THREADED)(benchmark::State& state) {
  auto thread_count = static_cast<uint16_t>(state.range(1));
  if (thread_count == 1) {
    read_non_atomic_random_single_threaded(state);
  } else {
    read_non_atomic_random_multi_threaded(state, thread_count);
  }
}

BENCHMARK_DEFINE_F(FileIOMicroReadBenchmarkFixture, PREAD_ATOMIC_SEQUENTIAL_THREADED)(benchmark::State& state) {
  auto thread_count = static_cast<uint16_t>(state.range(1));
  if (thread_count == 1) {
    pread_atomic_single_threaded(state);
  } else {
    pread_atomic_multi_threaded(state, thread_count);
  }
}

BENCHMARK_DEFINE_F(FileIOMicroReadBenchmarkFixture, PREAD_ATOMIC_RANDOM_THREADED)(benchmark::State& state) {
  auto thread_count = static_cast<uint16_t>(state.range(1));
  if (thread_count == 1) {
    pread_atomic_random_single_threaded(state);
  } else {
    pread_atomic_random_multi_threaded(state, thread_count);
  }
}

BENCHMARK_DEFINE_F(FileIOMicroReadBenchmarkFixture, IN_MEMORY_READ_SEQUENTIAL)(benchmark::State& state) {
  for (auto _ : state) {
    state.PauseTiming();
    auto read_data = std::vector<uint32_t>{};
    read_data.resize(NUMBER_OF_ELEMENTS);

    state.ResumeTiming();

    for (auto index = size_t{0}; index < NUMBER_OF_ELEMENTS; ++index) {
      read_data[index] = numbers[index];
    }

    state.PauseTiming();
    const auto sum = std::accumulate(read_data.begin(), read_data.end(), uint64_t{0});

    Assert(control_sum == sum, "Sanity check failed: Not the same result");
    Assert(&read_data != &numbers, "Sanity check failed: Same reference");

    state.ResumeTiming();
  }
}

BENCHMARK_DEFINE_F(FileIOMicroReadBenchmarkFixture, IN_MEMORY_READ_RANDOM)(benchmark::State& state) {
  for (auto _ : state) {
    state.PauseTiming();
    const auto random_indices = generate_random_indexes(NUMBER_OF_ELEMENTS);
    auto read_data = std::vector<uint32_t>{};
    read_data.resize(NUMBER_OF_ELEMENTS);
    state.ResumeTiming();

    for (auto index = size_t{0}; index < NUMBER_OF_ELEMENTS; ++index) {
      read_data[index] = numbers[random_indices[index]];
    }

    state.PauseTiming();
    const auto sum = std::accumulate(read_data.begin(), read_data.end(), uint64_t{0});

    Assert(control_sum == static_cast<uint64_t>(sum), "Sanity check failed: Not the same result");
    Assert(&read_data[0] != &numbers[random_indices[0]], "Sanity check failed: Same reference");

    state.ResumeTiming();
  }
}

// Arguments are file size in MB
BENCHMARK_REGISTER_F(FileIOMicroReadBenchmarkFixture, READ_NON_ATOMIC_SEQUENTIAL_THREADED)
    ->ArgsProduct({{10, 100, 1000}, {1, 2, 4, 6, 8, 16, 32}})
    ->UseRealTime();
BENCHMARK_REGISTER_F(FileIOMicroReadBenchmarkFixture, READ_NON_ATOMIC_RANDOM_THREADED)
    ->ArgsProduct({{10, 100, 1000}, {1, 2, 4, 6, 8, 16, 32}})
    ->UseRealTime();
BENCHMARK_REGISTER_F(FileIOMicroReadBenchmarkFixture, PREAD_ATOMIC_SEQUENTIAL_THREADED)
    ->ArgsProduct({{10, 100, 1000}, {1, 2, 4, 6, 8, 16, 32}})
    ->UseRealTime();
BENCHMARK_REGISTER_F(FileIOMicroReadBenchmarkFixture, PREAD_ATOMIC_RANDOM_THREADED)
    ->ArgsProduct({{10, 100, 1000}, {1, 2, 4, 6, 8, 16, 32}})
    ->UseRealTime();
BENCHMARK_REGISTER_F(FileIOMicroReadBenchmarkFixture, IN_MEMORY_READ_SEQUENTIAL)->Arg(10)->Arg(100)->Arg(1000);
BENCHMARK_REGISTER_F(FileIOMicroReadBenchmarkFixture, IN_MEMORY_READ_RANDOM)->Arg(10)->Arg(100)->Arg(1000);

}  // namespace hyrise
