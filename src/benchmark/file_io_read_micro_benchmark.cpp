#include <fcntl.h>
#ifdef __linux__
#include <libaio.h>
#endif
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
         close_file_and_return_error_message(fd, "Read error: ", errno));
}

void read_data_randomly_using_read(const size_t from, const size_t to, int32_t fd, uint32_t* read_data_start,
                                   const std::vector<uint32_t>& random_indices) {
  const auto uint32_t_size = ssize_t{sizeof(uint32_t)};

  // TODO(everyone): Randomize inidzes to not read all the data but really randomize the reads to read same amount but
  //  incl possible duplicates
  for (auto index = from; index < to; ++index) {
    lseek(fd, uint32_t_size * random_indices[index], SEEK_SET);
    Assert((read(fd, read_data_start + index, uint32_t_size) == uint32_t_size),
           close_file_and_return_error_message(fd, "Read error: ", errno));
  }
}

void read_data_using_pread(const size_t from, const size_t to, int32_t fd, uint32_t* read_data_start) {
  const auto uint32_t_size = ssize_t{sizeof(uint32_t)};
  const auto bytes_to_read = static_cast<ssize_t>(uint32_t_size * (to - from));
  Assert((pread(fd, read_data_start + from, bytes_to_read, from * uint32_t_size) == bytes_to_read),
         close_file_and_return_error_message(fd, "Read error: ", errno));
}

void read_data_randomly_using_pread(const size_t from, const size_t to, int32_t fd, uint32_t* read_data_start,
                                    const std::vector<uint32_t>& random_indices) {
  const auto uint32_t_size = ssize_t{sizeof(uint32_t)};

  lseek(fd, 0, SEEK_SET);
  // TODO(everyone): Randomize inidzes to not read all the data but really randomize the reads to read same amount but
  //  incl possible duplicates
  for (auto index = from; index < to; ++index) {
    Assert((pread(fd, read_data_start + index, uint32_t_size, uint32_t_size * random_indices[index]) == uint32_t_size),
           close_file_and_return_error_message(fd, "Read error: ", errno));
  }
}

#ifdef __linux__
void read_data_using_libaio(const size_t thread_from, const size_t thread_to, int32_t fd, uint32_t* read_data_start) {
  const auto uint32_t_size = ssize_t{sizeof(uint32_t)};
  const auto REQUEST_COUNT = uint32_t{64};
  const auto NUMBER_OF_ELEMENTS_PER_THREAD = (thread_to - thread_from);

  io_context_t ctx;
  memset(&ctx, 0, sizeof(ctx));
  io_setup(REQUEST_COUNT, &ctx);

  auto batch_size_thread =
      static_cast<uint64_t>(std::ceil(static_cast<float>(NUMBER_OF_ELEMENTS_PER_THREAD) / REQUEST_COUNT));

  auto iocbs = std::vector<iocb>(REQUEST_COUNT);
  auto iocb_list = std::vector<iocb*>(REQUEST_COUNT);

  for (auto index = size_t{0}; index < REQUEST_COUNT; ++index) {
    auto from = batch_size_thread * index + thread_from;
    auto to = from + batch_size_thread;
    if (to >= NUMBER_OF_ELEMENTS_PER_THREAD) {
      to = NUMBER_OF_ELEMENTS_PER_THREAD;
    }

    // io_prep_pread(struct iocb *iocb, int fd, void *buf, size_t count, long long offset);
    io_prep_pread(&iocbs[index], fd, read_data_start + from, batch_size_thread * uint32_t_size, from * uint32_t_size);
    iocb_list[index] = &iocbs[index];
  }

  auto return_value = io_submit(ctx, REQUEST_COUNT, iocb_list.data());
  Assert(return_value == REQUEST_COUNT,
         close_file_and_return_error_message(fd, "Asynchronous read using io_submit failed.", return_value));

  auto events = std::vector<io_event>(REQUEST_COUNT);
  auto events_count = io_getevents(ctx, REQUEST_COUNT, REQUEST_COUNT, events.data(), NULL);
  Assert(events_count == REQUEST_COUNT,
         close_file_and_return_error_message(fd, "Asynchronous read using io_getevents failed. ", events_count));

  io_destroy(ctx);
}

void read_data_randomly_using_libaio(const size_t thread_from, const size_t thread_to, int32_t fd,
                                     uint32_t* read_data_start, const std::vector<uint32_t>& random_indices) {
  const auto uint32_t_size = ssize_t{sizeof(uint32_t)};
  const auto REQUEST_COUNT = uint32_t{64};
  const auto NUMBER_OF_ELEMENTS_PER_THREAD = (thread_to - thread_from);

  io_context_t ctx;
  memset(&ctx, 0, sizeof(ctx));
  io_setup(REQUEST_COUNT, &ctx);

  auto iocbs = std::vector<iocb>(REQUEST_COUNT);
  auto iocb_list = std::vector<iocb*>(REQUEST_COUNT);

  for (auto index = size_t{0}; index < NUMBER_OF_ELEMENTS_PER_THREAD; ++index) {
    auto request_index = index % REQUEST_COUNT;

    if ((index > 0 && request_index == 0)) {
      auto return_value = io_submit(ctx, REQUEST_COUNT, iocb_list.data());
      Assert(return_value == REQUEST_COUNT,
             close_file_and_return_error_message(fd, "Asynchronous read using io_submit failed.", return_value));

      auto events = std::vector<io_event>(REQUEST_COUNT);
      auto events_count = io_getevents(ctx, REQUEST_COUNT, REQUEST_COUNT, events.data(), NULL);
      Assert(events_count == REQUEST_COUNT,
             close_file_and_return_error_message(fd, "Asynchronous read using io_getevents failed. ", events_count));
    }

    // io_prep_pread(struct iocb *iocb, int fd, void *buf, size_t count, long long offset);
    io_prep_pread(&iocbs[request_index], fd, read_data_start + thread_from + index, uint32_t_size,
                  uint32_t_size * random_indices[thread_from + index]);
    iocb_list[request_index] = &iocbs[request_index];
  }

  auto return_value = io_submit(ctx, REQUEST_COUNT, iocb_list.data());
  Assert(return_value == REQUEST_COUNT,
         close_file_and_return_error_message(fd, "Asynchronous read using io_submit failed.", return_value));

  auto events = std::vector<io_event>(REQUEST_COUNT);
  auto events_count = io_getevents(ctx, REQUEST_COUNT, REQUEST_COUNT, events.data(), NULL);
  Assert(events_count == REQUEST_COUNT,
         close_file_and_return_error_message(fd, "Asynchronous read using io_getevents failed. ", events_count));

  io_destroy(ctx);
}
#endif

// TODO(everyone): Reduce LOC by making this function more modular (do not repeat it with different function inputs).
void FileIOMicroReadBenchmarkFixture::read_non_atomic_multi_threaded(benchmark::State& state, uint16_t thread_count) {
  auto filedescriptors = std::vector<int32_t>(thread_count);
  for (auto index = size_t{0}; index < thread_count; ++index) {
    auto fd = int32_t{};
    Assert(((fd = open(filename.c_str(), O_RDONLY)) >= 0),
           close_file_and_return_error_message(fd, "Open error: ", errno));
    filedescriptors[index] = fd;
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

    for (auto index = size_t{0}; index < thread_count; ++index) {
      auto from = batch_size * index;
      auto to = from + batch_size;
      if (to >= NUMBER_OF_ELEMENTS) {
        to = NUMBER_OF_ELEMENTS;
      }
      threads[index] = (std::thread(read_data_using_read, from, to, filedescriptors[index], read_data_start));
    }

    for (auto index = size_t{0}; index < thread_count; ++index) {
      // Explain: Blocks the current thread until the thread identified by *this finishes its execution
      threads[index].join();
    }
    state.PauseTiming();

    const auto sum = std::accumulate(read_data.begin(), read_data.end(), uint64_t{0});
    Assert(control_sum == sum, "Sanity check failed: Not the same result");
    state.ResumeTiming();
  }

  for (auto index = size_t{0}; index < thread_count; ++index) {
    close(filedescriptors[index]);
  }
}

void FileIOMicroReadBenchmarkFixture::read_non_atomic_single_threaded(benchmark::State& state) {
  auto fd = int32_t{};
  Assert(((fd = open(filename.c_str(), O_RDONLY)) >= 0),
         close_file_and_return_error_message(fd, "Open error: ", errno));

  for (auto _ : state) {
    state.PauseTiming();

    micro_benchmark_clear_disk_cache();
    auto read_data = std::vector<uint32_t>{};
    read_data.resize(NUMBER_OF_ELEMENTS);

    state.ResumeTiming();

    lseek(fd, 0, SEEK_SET);
    Assert((read(fd, std::data(read_data), NUMBER_OF_BYTES) == NUMBER_OF_BYTES),
           close_file_and_return_error_message(fd, "Read error: ", errno));

    state.PauseTiming();

    const auto sum = std::accumulate(read_data.begin(), read_data.end(), uint64_t{0});
    Assert(control_sum == sum, "Sanity check failed: Not the same result");

    state.ResumeTiming();
  }

  close(fd);
}

void FileIOMicroReadBenchmarkFixture::read_non_atomic_random_single_threaded(benchmark::State& state) {
  auto fd = int32_t{};
  Assert(((fd = open(filename.c_str(), O_RDONLY)) >= 0),
         close_file_and_return_error_message(fd, "Open error: ", errno));

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
             close_file_and_return_error_message(fd, "Read error: ", errno));
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
  for (auto index = size_t{0}; index < thread_count; ++index) {
    auto fd = int32_t{};
    Assert(((fd = open(filename.c_str(), O_RDONLY)) >= 0),
           close_file_and_return_error_message(fd, "Open error: ", errno));
    filedescriptors[index] = fd;
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
    for (auto index = size_t{0}; index < thread_count; ++index) {
      auto from = batch_size * index;
      auto to = from + batch_size;
      if (to >= NUMBER_OF_ELEMENTS) {
        to = NUMBER_OF_ELEMENTS;
      }
      threads[index] = (std::thread(read_data_randomly_using_read, from, to, filedescriptors[index],
                                    std::data(read_data), random_indices));
    }

    for (auto index = size_t{0}; index < thread_count; ++index) {
      // Explain: Blocks the current thread until the thread identified by *this finishes its execution
      threads[index].join();
    }
    state.PauseTiming();

    const auto sum = std::accumulate(read_data.begin(), read_data.end(), uint64_t{0});
    Assert(control_sum == sum, "Sanity check failed: Not the same result");

    state.ResumeTiming();
  }

  for (auto index = size_t{0}; index < thread_count; ++index) {
    close(filedescriptors[index]);
  }
}

void FileIOMicroReadBenchmarkFixture::pread_atomic_single_threaded(benchmark::State& state) {
  auto fd = int32_t{};
  Assert(((fd = open(filename.c_str(), O_RDONLY)) >= 0),
         close_file_and_return_error_message(fd, "Open error: ", errno));

  for (auto _ : state) {
    state.PauseTiming();

    micro_benchmark_clear_disk_cache();
    auto read_data = std::vector<uint32_t>{};
    read_data.resize(NUMBER_OF_ELEMENTS);
    state.ResumeTiming();

    Assert((pread(fd, std::data(read_data), NUMBER_OF_BYTES, 0) == NUMBER_OF_BYTES),
           close_file_and_return_error_message(fd, "Read error: ", errno));

    state.PauseTiming();

    const auto sum = std::accumulate(read_data.begin(), read_data.end(), uint64_t{0});
    Assert(control_sum == sum, "Sanity check failed: Not the same result");
    state.ResumeTiming();
  }

  close(fd);
}

void FileIOMicroReadBenchmarkFixture::pread_atomic_multi_threaded(benchmark::State& state, uint16_t thread_count) {
  auto filedescriptors = std::vector<int32_t>(thread_count);
  for (auto index = size_t{0}; index < thread_count; ++index) {
    auto fd = int32_t{};
    Assert(((fd = open(filename.c_str(), O_RDONLY)) >= 0), close_file_and_return_error_message(fd, "Open error: ", errno));
    filedescriptors[index] = fd;
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

    for (auto index = size_t{0}; index < thread_count; ++index) {
      auto from = batch_size * index;
      auto to = from + batch_size;
      if (to >= NUMBER_OF_ELEMENTS) {
        to = NUMBER_OF_ELEMENTS;
      }
      threads[index] = (std::thread(read_data_using_pread, from, to, filedescriptors[index], read_data_start));
    }

    for (auto index = size_t{0}; index < thread_count; ++index) {
      // Explain: Blocks the current thread until the thread identified by *this finishes its execution
      threads[index].join();
    }
    state.PauseTiming();

    const auto sum = std::accumulate(read_data.begin(), read_data.end(), uint64_t{0});
    Assert(control_sum == sum, "Sanity check failed: Not the same result");
    state.ResumeTiming();
  }

  for (auto index = size_t{0}; index < thread_count; ++index) {
    close(filedescriptors[index]);
  }
}

void FileIOMicroReadBenchmarkFixture::pread_atomic_random_single_threaded(benchmark::State& state) {
  auto fd = int32_t{};
  Assert(((fd = open(filename.c_str(), O_RDONLY)) >= 0),
         close_file_and_return_error_message(fd, "Open error: ", errno));

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
             close_file_and_return_error_message(fd, "Read error: ", errno));
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
  auto filedescriptors = std::vector<int32_t>(thread_count);
  for (auto index = size_t{0}; index < thread_count; ++index) {
    auto fd = int32_t{};
    Assert(((fd = open(filename.c_str(), O_RDONLY)) >= 0), close_file_and_return_error_message(fd, "Open error: ", errno));
    filedescriptors[index] = fd;
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
    for (auto index = size_t{0}; index < thread_count; ++index) {
      auto from = batch_size * index;
      auto to = from + batch_size;
      if (to >= NUMBER_OF_ELEMENTS) {
        to = NUMBER_OF_ELEMENTS;
      }
      threads[index] = (std::thread(read_data_randomly_using_pread, from, to, filedescriptors[index], std::data(read_data), random_indices));
    }

    for (auto index = size_t{0}; index < thread_count; ++index) {
      // Explain: Blocks the current thread until the thread identified by *this finishes its execution
      threads[index].join();
    }
    state.PauseTiming();

    const auto sum = std::accumulate(read_data.begin(), read_data.end(), uint64_t{0});
    Assert(control_sum == sum, "Sanity check failed: Not the same result");

    state.ResumeTiming();
  }

  for (auto index = size_t{0}; index < thread_count; ++index) {
    close(filedescriptors[index]);
  }
}

#ifdef __linux__
void FileIOMicroReadBenchmarkFixture::libaio_sequential_read_single_threaded(benchmark::State& state) {
  auto fd = int32_t{};
  Assert(((fd = open(filename.c_str(), O_RDONLY)) >= 0),
         close_file_and_return_error_message(fd, "Open error: ", errno));

  // The context is shared among threads.
  io_context_t ctx;
  memset(&ctx, 0, sizeof(ctx));
  // long io_setup(unsigned int nr_events, aio_context_t *ctx_idp);
  int ret = io_setup(1, &ctx);
  if (ret < 0) {
    std::cerr << "Error initializing libaio context" << std::endl;
    exit(1);
  }

  for (auto _ : state) {
    state.PauseTiming();
    micro_benchmark_clear_disk_cache();
    auto read_data = std::vector<uint32_t>{};
    read_data.resize(NUMBER_OF_ELEMENTS);
    state.ResumeTiming();

    // Creating the request
    struct iocb request;
    /*
    request.aio_lio_opcode = IO_CMD_PREAD;
    request.aio_fildes = fd;
    request.buf  = std::data(read_data);
    request.nbytes  = NUMBER_OF_BYTES;
    request.offset  = 0 ;
    is done by the convenience function 'io_prep_pread'
    */
    io_prep_pread(&request, fd, std::data(read_data), NUMBER_OF_BYTES, 0);

    // Submit the request.
    struct iocb* requests[1] = {&request};
    io_submit(ctx, 1, requests);

    struct io_event event;
    io_getevents(ctx, 1, 1, &event, NULL);

    ret = event.res;
    if (ret < 0) {
      std::cerr << "Read error: " << strerror(errno) << std::endl;
      exit(1);
    }

    state.PauseTiming();
    const auto sum = std::accumulate(read_data.begin(), read_data.end(), uint64_t{0});
    Assert(control_sum == sum, "Sanity check failed: Not the same result");
    state.ResumeTiming();
  }

  io_destroy(ctx);
  close(fd);
}

void FileIOMicroReadBenchmarkFixture::libaio_sequential_read_multi_threaded(benchmark::State& state,
                                                                            uint16_t thread_count) {
  auto filedescriptors = std::vector<int32_t>(thread_count);
  for (auto index = size_t{0}; index < thread_count; ++index) {
    auto fd = int32_t{};
    Assert(((fd = open(filename.c_str(), O_RDONLY)) >= 0), close_file_and_return_error_message(fd, "Open error: ", errno));
    filedescriptors[index] = fd;
  }

  auto threads = std::vector<std::thread>(thread_count);
  auto batch_size = static_cast<uint64_t>(std::ceil(static_cast<float>(NUMBER_OF_ELEMENTS) / thread_count));

  for (auto _ : state) {
    state.PauseTiming();
    auto read_data = std::vector<uint32_t>{};
    read_data.resize(NUMBER_OF_ELEMENTS);
    micro_benchmark_clear_disk_cache();
    state.ResumeTiming();

    for (auto index = size_t{0}; index < thread_count; ++index) {
      auto from = batch_size * index;
      auto to = from + batch_size;
      if (to >= NUMBER_OF_ELEMENTS) {
        to = NUMBER_OF_ELEMENTS;
      }
      threads[index] = (std::thread(read_data_using_libaio, from, to, filedescriptors[index], std::data(read_data)));
    }

    for (auto index = size_t{0}; index < thread_count; ++index) {
      threads[index].join();
    }

    state.PauseTiming();
    const auto sum = std::accumulate(read_data.begin(), read_data.end(), uint64_t{0});
    Assert(control_sum == sum, "Sanity check failed: Not the same result. Got: " + std::to_string(sum) +
                                   " Expected: " + std::to_string(control_sum) + ".");
    state.ResumeTiming();
  }

  for (auto index = size_t{0}; index < thread_count; ++index) {
    close(filedescriptors[index]);
  }
}

void FileIOMicroReadBenchmarkFixture::libaio_random_read(benchmark::State& state, uint16_t thread_count) {
  auto filedescriptors = std::vector<int32_t>(thread_count);
  for (auto index = size_t{0}; index < thread_count; ++index) {
    auto fd = int32_t{};
    Assert(((fd = open(filename.c_str(), O_RDONLY)) >= 0), close_file_and_return_error_message(fd, "Open error: ", errno));
    filedescriptors[index] = fd;
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

    for (auto index = size_t{0}; index < thread_count; ++index) {
      auto from = batch_size * index;
      auto to = from + batch_size;
      if (to >= NUMBER_OF_ELEMENTS) {
        to = NUMBER_OF_ELEMENTS;
      }
      threads[index] = (std::thread(read_data_randomly_using_libaio, from, to, filedescriptors[index],
                                    std::data(read_data), random_indices));
    }

    for (auto index = size_t{0}; index < thread_count; ++index) {
      threads[index].join();
    }

    state.PauseTiming();
    const auto sum = std::accumulate(read_data.begin(), read_data.end(), uint64_t{0});
    Assert(control_sum == sum, "Sanity check failed: Not the same result. Got: " + std::to_string(sum) +
                                   " Expected: " + std::to_string(control_sum) + ".");
    state.ResumeTiming();
  }

  for (auto index = size_t{0}; index < thread_count; ++index) {
    close(filedescriptors[index]);
  }
}
#endif

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

#ifdef __linux__
BENCHMARK_DEFINE_F(FileIOMicroReadBenchmarkFixture, LIBAIO_SEQUENTIAL_THREADED)(benchmark::State& state) {
  auto thread_count = static_cast<uint16_t>(state.range(1));
  if (thread_count == 1) {
    libaio_sequential_read_single_threaded(state);
  } else {
    libaio_sequential_read_multi_threaded(state, thread_count);
  }
}

BENCHMARK_DEFINE_F(FileIOMicroReadBenchmarkFixture, LIBAIO_RANDOM_THREADED)(benchmark::State& state) {
  auto thread_count = static_cast<uint16_t>(state.range(1));
  libaio_random_read(state, thread_count);
}
#endif

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
    ->ArgsProduct({{10, 100, 1000}, {1, 2, 4, 8, 16, 32, 48}})
    ->UseRealTime();
BENCHMARK_REGISTER_F(FileIOMicroReadBenchmarkFixture, READ_NON_ATOMIC_RANDOM_THREADED)
    ->ArgsProduct({{10, 100, 1000}, {1, 2, 4, 8, 16, 32, 48}})
    ->UseRealTime();
BENCHMARK_REGISTER_F(FileIOMicroReadBenchmarkFixture, PREAD_ATOMIC_SEQUENTIAL_THREADED)
    ->ArgsProduct({{10, 100, 1000}, {1, 2, 4, 8, 16, 32, 48}})
    ->UseRealTime();
BENCHMARK_REGISTER_F(FileIOMicroReadBenchmarkFixture, PREAD_ATOMIC_RANDOM_THREADED)
    ->ArgsProduct({{10, 100, 1000}, {1, 2, 4, 8, 16, 32, 48}})
    ->UseRealTime();

#ifdef __linux__
BENCHMARK_REGISTER_F(FileIOMicroReadBenchmarkFixture, LIBAIO_SEQUENTIAL_THREADED)
    ->ArgsProduct({{10, 100, 1000}, {1, 2, 4, 8, 16, 32, 48}})
    ->UseRealTime();
BENCHMARK_REGISTER_F(FileIOMicroReadBenchmarkFixture, LIBAIO_RANDOM_THREADED)
    ->ArgsProduct({{10, 100, 1000}, {1, 2, 4, 8, 16, 32, 48}})
    ->UseRealTime();
#endif
BENCHMARK_REGISTER_F(FileIOMicroReadBenchmarkFixture, IN_MEMORY_READ_SEQUENTIAL)->Arg(1000)->UseRealTime();
BENCHMARK_REGISTER_F(FileIOMicroReadBenchmarkFixture, IN_MEMORY_READ_RANDOM)->Arg(1000)->UseRealTime();

}  // namespace hyrise
