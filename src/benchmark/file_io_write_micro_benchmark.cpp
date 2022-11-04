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
  void SetUp(::benchmark::State& state) override {
    ssize_t BUFFER_SIZE = state.range(0);
		// each int32_t contains four bytes
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
};

BENCHMARK_DEFINE_F(FileIOWriteMicroBenchmarkFixture, WRITE_NON_ATOMIC)(benchmark::State& state) {// open file
  int32_t fd;
  if ((fd = open("file.txt", O_WRONLY)) < 0) {
		std::cout << "open error " << errno << std::endl;
  }
  for (auto _ : state) {
    if (write(fd, std::data(data_to_write), state.range(0)) != state.range(0)) {
			std::cout << "write error " << errno << std::endl;
		}
  }
}

BENCHMARK_DEFINE_F(FileIOWriteMicroBenchmarkFixture, PWRITE_ATOMIC)(benchmark::State& state) {
  int32_t fd;
	if ((fd = open("file.txt", O_WRONLY)) < 0) {
		std::cout << "open error " << errno << std::endl;
	}
	for (auto _ : state) {
		if (pwrite(fd, std::data(data_to_write), state.range(0), 0) != state.range(0)) {
			std::cout << "write error " << errno << std::endl;
		}
	}
}

BENCHMARK_REGISTER_F(FileIOWriteMicroBenchmarkFixture, WRITE_NON_ATOMIC)->Arg(10000000)->Arg(100000000);
BENCHMARK_REGISTER_F(FileIOWriteMicroBenchmarkFixture, PWRITE_ATOMIC)->Arg(10000000)->Arg(100000000);

}  // namespace hyrise