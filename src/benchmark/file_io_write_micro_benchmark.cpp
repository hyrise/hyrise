#include "micro_benchmark_basic_fixture.hpp"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <algorithm>

namespace hyrise {

// Defining the base fixture class
class FileIOWriteMicroBenchmarkFixture : public MicroBenchmarkBasicFixture {
 public:
  void SetUp(::benchmark::State& /*state*/) override {
    // 32 bit * 250 000 000 entry equals 1GB data
    int32_t vector_element_count = BUFFER_SIZE / 4;
    data_to_write = std::vector<int32_t>(vector_element_count, 42);
    if (creat("file.txt", O_WRONLY) < 1) {
      std::cout << "create error" << std::endl;
    }
    chmod("file.txt", S_IRWXU);  // enables owner to rwx file
  }

  // Required to avoid resetting of StorageManager in MicroBenchmarkBasicFixture::TearDown()
  void TearDown(::benchmark::State& /*state*/) override {
    // TODO Error handling
    std::remove("file.txt");
  }

 protected:
  std::vector<int32_t> data_to_write;
  const ssize_t BUFFER_SIZE = 100000000;
};

BENCHMARK_F(FileIOWriteMicroBenchmarkFixture, WRITE_NON_ATOMIC)(benchmark::State& state) {
  for (auto _ : state) {
    // open file
    int32_t fd;
    if ((fd = open("file.txt", O_WRONLY)) < 0)
      std::cout << "open error" << std::endl;
    if (write(fd, std::data(data_to_write), BUFFER_SIZE) != BUFFER_SIZE)
      std::cout << "write error" << std::endl;
  }
}

BENCHMARK_F(FileIOWriteMicroBenchmarkFixture, PWRITE_ATOMIC)(benchmark::State& state) {
  for (auto _ : state) {
    // open file
    int32_t fd;
    if ((fd = open("file.txt", O_WRONLY)) < 0)
      std::cout << "open error" << std::endl;
    if (pwrite(fd, std::data(data_to_write), BUFFER_SIZE, 0) != BUFFER_SIZE)
      std::cout << "write error" << std::endl;
  }
}

}  // namespace hyrise