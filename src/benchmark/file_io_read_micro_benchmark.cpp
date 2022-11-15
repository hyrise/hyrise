#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <algorithm>
#include <numeric>
#include <random>
#include <iterator>
#include "micro_benchmark_basic_fixture.hpp"

namespace hyrise {

const int32_t MB = 1000000;

class FileIOMicroReadBenchmarkFixture : public MicroBenchmarkBasicFixture {
 public:
  uint64_t control_sum = uint64_t{0};
  std::vector<uint32_t> numbers;
  std::vector<uint32_t> random_indices;

  void SetUp(::benchmark::State& state) override {
    // TODO: Make setup/teardown global per file size to improve benchmark speed
    // TODO: Generate vector for each loop in state and not only once per state
    ssize_t BUFFER_SIZE_MB = state.range(0);

    // each int32_t contains four bytes
    uint32_t vector_element_count = (BUFFER_SIZE_MB * MB) / 4;
    numbers = std::vector<uint32_t>(vector_element_count);
    for (size_t index = 0; index < vector_element_count; ++index) {
      numbers[index] = std::rand() % UINT32_MAX;
    }
    control_sum = std::accumulate(numbers.begin(), numbers.end(), uint64_t{0});

    random_indices = std::vector<uint32_t>(vector_element_count);
    std::iota(std::begin(random_indices), std::end(random_indices), 0);
    std::random_device rd;
    std::mt19937 engine(rd());
    std::shuffle(random_indices.begin(), random_indices.end(), engine);

    int32_t fd;
    if ((fd = creat("file.txt", O_WRONLY)) < 1) {
      std::cout << "create error" << std::endl;
    }
    chmod("file.txt", S_IRWXU);  // enables owner to rwx file
    if (write(fd, std::data(numbers), BUFFER_SIZE_MB * MB) != BUFFER_SIZE_MB * MB) {
      std::cout << "write error" << std::endl;
    }
    close(fd);
  }

  void TearDown(::benchmark::State& /*state*/) override {
    // TODO: Error handling
    std::remove("file.txt");
  }

 protected:
};

BENCHMARK_DEFINE_F(FileIOMicroReadBenchmarkFixture, READ_NON_ATOMIC_SEQUENTIAL)(benchmark::State& state) {  // open file
  int32_t fd;
  if ((fd = open("file.txt", O_RDONLY)) < 0) {
    std::cout << "open error " << errno << std::endl;
  }
  const int32_t NUMBER_OF_BYTES = state.range(0) * MB;

  for (auto _ : state) {
    state.PauseTiming();
    micro_benchmark_clear_disk_cache();
    std::vector<uint32_t> read_data;

    auto read_data_size = NUMBER_OF_BYTES / 4;
    read_data.resize(read_data_size);
    state.ResumeTiming();

    for(auto index = size_t{0}; index < static_cast<size_t>(read_data_size); ++index){
      lseek(fd, sizeof (uint32_t) * index, SEEK_SET);

      if (read(fd, std::data(read_data) + index, sizeof (uint32_t)) != sizeof (uint32_t)) {
        Fail("read error: " + strerror(errno));
      }
    }

    state.PauseTiming();
    auto sum = std::accumulate(read_data.begin(), read_data.end(), uint64_t{0});
    // sum == 0 because read vector is empty
    Assert(control_sum == sum, "Sanity check failed: Not the same result");
    state.ResumeTiming();
  }
}

BENCHMARK_DEFINE_F(FileIOMicroReadBenchmarkFixture, READ_NON_ATOMIC_RANDOM)(benchmark::State& state) {  // open file
  int32_t fd;
  if ((fd = open("file.txt", O_RDONLY)) < 0) {
    std::cout << "open error " << errno << std::endl;
  }
  const int32_t NUMBER_OF_BYTES = state.range(0) * MB;

  for (auto _ : state) {
    state.PauseTiming();
    micro_benchmark_clear_disk_cache();
    std::vector<uint32_t> read_data;
    auto read_data_size = NUMBER_OF_BYTES / 4;
    read_data.resize(read_data_size);
    state.ResumeTiming();

    lseek(fd, 0, SEEK_SET);

    for(auto index = size_t{0}; index < static_cast<size_t>(read_data_size); ++index){
      // here
      lseek(fd, sizeof (uint32_t) * random_indices[index], SEEK_SET);

      if (read(fd, std::data(read_data) + index, sizeof (uint32_t)) != sizeof (uint32_t)) {
        Fail("read error: " + strerror(errno));
      }
    }

    state.PauseTiming();
    auto sum = std::accumulate(read_data.begin(), read_data.end(), uint64_t{0});
    // sum == 0 because read vector is empty
    Assert(control_sum == sum, "Sanity check failed: Not the same result");
    state.ResumeTiming();
  }
}

BENCHMARK_DEFINE_F(FileIOMicroReadBenchmarkFixture, PREAD_ATOMIC_SEQUENTIAL)(benchmark::State& state) {
  int32_t fd;
  if ((fd = open("file.txt", O_RDONLY)) < 0) {
    std::cout << "open error " << errno << std::endl;
  }
  const int32_t NUMBER_OF_BYTES = state.range(0) * MB;

  for (auto _ : state) {
    state.PauseTiming();
    micro_benchmark_clear_disk_cache();
    std::vector<uint32_t> read_data;
    read_data.resize(NUMBER_OF_BYTES / 4);
    state.ResumeTiming();

    if (pread(fd, std::data(read_data), NUMBER_OF_BYTES, 0) != NUMBER_OF_BYTES) {
      Fail("read error: " + strerror(errno));
    }

    state.PauseTiming();
    auto sum = std::accumulate(read_data.begin(), read_data.end(), uint64_t{0});
    Assert(control_sum == sum, "Sanity check failed: Not the same result");

    state.ResumeTiming();
  }
}



BENCHMARK_DEFINE_F(FileIOMicroReadBenchmarkFixture, IN_MEMORY_READ_SEQUENTIAL)(benchmark::State& state) {  // open file
  for (auto _ : state) {
    const int32_t NUMBER_OF_BYTES = state.range(0) * MB;

    state.PauseTiming();
    std::vector<uint32_t> read_data;
    auto read_data_size = NUMBER_OF_BYTES / 4;
    read_data.resize(read_data_size);
    state.ResumeTiming();

    for(auto index = size_t{0}; index < static_cast<size_t>(read_data_size); ++index){
      read_data[index] = numbers[index];
    }

    state.PauseTiming();
    auto sum = std::accumulate(read_data.begin(), read_data.end(), uint64_t{0});

    Assert(control_sum == sum, "Sanity check failed: Not the same result");
    Assert(&read_data != &numbers, "Sanity check failed: Same reference");

    state.ResumeTiming();
  }
}

BENCHMARK_DEFINE_F(FileIOMicroReadBenchmarkFixture, IN_MEMORY_READ_RANDOM)(benchmark::State& state) {  // open file
  for (auto _ : state) {
    const int32_t NUMBER_OF_BYTES = state.range(0) * MB;

    state.PauseTiming();
    std::vector<uint32_t> read_data;
    auto random_read_amount = static_cast<size_t>(NUMBER_OF_BYTES / 4);
    read_data.resize(random_read_amount);
    state.ResumeTiming();

    for(auto index = size_t{0}; index < random_read_amount; ++index){
      read_data[index] = numbers[random_indices[index]];
    }

    state.PauseTiming();
    auto sum = std::accumulate(read_data.begin(), read_data.end(), uint64_t{0});

    Assert(control_sum == static_cast<uint64_t>(sum), "Sanity check failed: Not the same result");
    Assert(&read_data[0] != &numbers[random_indices[0]], "Sanity check failed: Same reference");

    state.ResumeTiming();
  }
}

// Arguments are file size in MB
BENCHMARK_REGISTER_F(FileIOMicroReadBenchmarkFixture, READ_NON_ATOMIC_SEQUENTIAL)->Arg(10);//->Arg(100)->Arg(1000);
BENCHMARK_REGISTER_F(FileIOMicroReadBenchmarkFixture, READ_NON_ATOMIC_RANDOM)->Arg(10);//->Arg(100)->Arg(1000);

//BENCHMARK_REGISTER_F(FileIOMicroReadBenchmarkFixture, PREAD_ATOMIC)->Arg(10)->Arg(100)->Arg(1000);
// BENCHMARK_REGISTER_F(FileIOMicroReadBenchmarkFixture, IN_MEMORY_READ_SEQUENTIAL)->Arg(10);//->Arg(100)->Arg(1000);
// BENCHMARK_REGISTER_F(FileIOMicroReadBenchmarkFixture, IN_MEMORY_READ_RANDOM)->Arg(10);//->Arg(100)->Arg(1000);


}  // namespace hyrise
