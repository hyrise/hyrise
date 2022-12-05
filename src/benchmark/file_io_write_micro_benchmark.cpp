#include "file_io_write_micro_benchmark.hpp"

#include <fcntl.h>
#include <unistd.h>
#include <algorithm>
#include <numeric>

namespace hyrise {

void write_data_using_write(const size_t from, const size_t to, int32_t fd, uint32_t* data_to_write_start) {
  const auto uint32_t_size = ssize_t{sizeof(uint32_t)};
  const auto bytes_to_write = static_cast<ssize_t>(uint32_t_size * (to - from));
  lseek(fd, from * uint32_t_size, SEEK_SET);
  Assert((write(fd, data_to_write_start + from, bytes_to_write) == bytes_to_write),
         fail_and_close_file(fd, "Write error: ", errno));
}

void write_data_using_pwrite(const size_t from, const size_t to, int32_t fd, uint32_t* data_to_write_start) {
  const auto uint32_t_size = ssize_t{sizeof(uint32_t)};
  const auto bytes_to_write = static_cast<ssize_t>(uint32_t_size * (to - from));
  Assert((pwrite(fd, data_to_write_start + from, bytes_to_write, from * uint32_t_size) == bytes_to_write),
         fail_and_close_file(fd, "Write error: ", errno));
}

void FileIOWriteMicroBenchmarkFixture::write_non_atomic_single_threaded(benchmark::State& state) {
  auto fd = int32_t{};
  Assert(((fd = open(filename, O_WRONLY)) >= 0), fail_and_close_file(fd, "Open error: ", errno));

  for (auto _ : state) {
    state.PauseTiming();
    micro_benchmark_clear_disk_cache();
    state.ResumeTiming();

    lseek(fd, 0, SEEK_SET);
    Assert((write(fd, std::data(data_to_write), NUMBER_OF_BYTES) == NUMBER_OF_BYTES),
           fail_and_close_file(fd, "Write error: ", errno));

    state.PauseTiming();
    sanity_check();
    state.ResumeTiming();
  }

  close(fd);
}

void FileIOWriteMicroBenchmarkFixture::write_non_atomic_multi_threaded(benchmark::State& state, uint16_t thread_count) {
  auto filedescriptors = std::vector<int32_t>(thread_count);
  for (auto i = size_t{0}; i < thread_count; i++) {
    auto fd = int32_t{};
    Assert(((fd = open(filename, O_WRONLY)) >= 0), fail_and_close_file(fd, "Open error: ", errno));
    filedescriptors[i] = fd;
  }

  auto threads = std::vector<std::thread>(thread_count);
  auto batch_size = static_cast<uint64_t>(std::ceil(static_cast<float>(NUMBER_OF_ELEMENTS) / thread_count));

  for (auto _ : state) {
    state.PauseTiming();
    micro_benchmark_clear_disk_cache();
    state.ResumeTiming();

    auto* data_to_write_start = std::data(data_to_write);

    for (auto i = size_t{0}; i < thread_count; i++) {
      auto from = batch_size * i;
      auto to = from + batch_size;
      if (to >= NUMBER_OF_ELEMENTS) {
        to = NUMBER_OF_ELEMENTS;
      }
      threads[i] = std::thread(write_data_using_write, from, to, filedescriptors[i], data_to_write_start);
    }

    for (auto i = size_t{0}; i < thread_count; i++) {
      //Blocks the current thread until the thread identified by *this finishes its execution
      threads[i].join();
    }

    state.PauseTiming();
    sanity_check();
    state.ResumeTiming();
  }

  for (auto i = size_t{0}; i < thread_count; i++) {
    close(filedescriptors[i]);
  }
}

void FileIOWriteMicroBenchmarkFixture::pwrite_atomic_single_threaded(benchmark::State& state) {
  auto fd = int32_t{};
  Assert(((fd = open(filename, O_WRONLY)) >= 0), fail_and_close_file(fd, "Open error: ", errno));

  for (auto _ : state) {
    state.PauseTiming();
    micro_benchmark_clear_disk_cache();
    state.ResumeTiming();

    lseek(fd, 0, SEEK_SET);
    if (pwrite(fd, std::data(data_to_write), NUMBER_OF_BYTES, 0) != NUMBER_OF_BYTES) {
      close(fd);
      Fail("Write error:" + std::strerror(errno));
    }

    state.PauseTiming();
    sanity_check();
    state.ResumeTiming();
  }

  close(fd);
}

void FileIOWriteMicroBenchmarkFixture::pwrite_atomic_multi_threaded(benchmark::State& state, uint16_t thread_count) {
  auto filedescriptors = std::vector<int32_t>(thread_count);
  for (auto i = size_t{0}; i < thread_count; i++) {
    auto fd = int32_t{};
    Assert(((fd = open(filename, O_WRONLY)) >= 0), fail_and_close_file(fd, "Open error: ", errno));
    filedescriptors[i] = fd;
  }

  auto threads = std::vector<std::thread>(thread_count);
  auto batch_size = static_cast<uint64_t>(std::ceil(static_cast<float>(NUMBER_OF_ELEMENTS) / thread_count));

  for (auto _ : state) {
    state.PauseTiming();
    micro_benchmark_clear_disk_cache();
    state.ResumeTiming();

    auto* data_to_write_start = std::data(data_to_write);

    for (auto i = size_t{0}; i < thread_count; i++) {
      auto from = batch_size * i;
      auto to = from + batch_size;
      if (to >= NUMBER_OF_ELEMENTS) {
        to = NUMBER_OF_ELEMENTS;
      }
      threads[i] = std::thread(write_data_using_pwrite, from, to, filedescriptors[i], data_to_write_start);
    }

    for (auto i = size_t{0}; i < thread_count; i++) {
      // Blocks the current thread until the thread identified by *this finishes its execution
      threads[i].join();
    }

    state.PauseTiming();
    sanity_check();
    state.ResumeTiming();
  }

  for (auto i = size_t{0}; i < thread_count; i++) {
    close(filedescriptors[i]);
  }
}

BENCHMARK_DEFINE_F(FileIOWriteMicroBenchmarkFixture, WRITE_NON_ATOMIC_THREADED)(benchmark::State& state) {
  auto thread_count = static_cast<uint16_t>(state.range(1));

  // for one thread run sequential implementation to avoid measuring unneccesary thread overhead
  if (thread_count == 1) {
    write_non_atomic_single_threaded(state);
  } else {
    write_non_atomic_multi_threaded(state, thread_count);
  }
}

BENCHMARK_DEFINE_F(FileIOWriteMicroBenchmarkFixture, PWRITE_ATOMIC_THREADED)(benchmark::State& state) {
  auto thread_count = static_cast<uint16_t>(state.range(1));

  // for one thread run sequential implementation to avoid measuring unneccesary thread overhead
  if (thread_count == 1) {
    pwrite_atomic_single_threaded(state);
  } else {
    pwrite_atomic_multi_threaded(state, thread_count);
  }
}

BENCHMARK_DEFINE_F(FileIOWriteMicroBenchmarkFixture, IN_MEMORY_WRITE)(benchmark::State& state) {
  std::vector<uint32_t> copy_of_contents;

  for (auto _ : state) {
    copy_of_contents = data_to_write;
    state.PauseTiming();
    Assert(std::equal(copy_of_contents.begin(), copy_of_contents.end(), data_to_write.begin()),
           "Sanity check failed: Not the same result");
    Assert(&copy_of_contents != &data_to_write, "Sanity check failed: Same reference");
    state.ResumeTiming();
  }
}

// Arguments are file size in MB
BENCHMARK_REGISTER_F(FileIOWriteMicroBenchmarkFixture, WRITE_NON_ATOMIC_THREADED)
    ->ArgsProduct({{10, 100, 1000}, {1, 2, 4, 8, 16, 32}})
    ->UseRealTime();
BENCHMARK_REGISTER_F(FileIOWriteMicroBenchmarkFixture, PWRITE_ATOMIC_THREADED)
    ->ArgsProduct({{10, 100, 1000}, {1, 2, 4, 8, 16, 32}})
    ->UseRealTime();
BENCHMARK_REGISTER_F(FileIOWriteMicroBenchmarkFixture, IN_MEMORY_WRITE)->Arg(10)->Arg(100)->Arg(1000)->UseRealTime();

}  // namespace hyrise
