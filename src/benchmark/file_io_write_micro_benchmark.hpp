#include "micro_benchmark_basic_fixture.hpp"

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <numeric>

namespace hyrise {

const auto MB = uint32_t{1'000'000};

class FileIOWriteMicroBenchmarkFixture : public MicroBenchmarkBasicFixture {
 public:
  void SetUp(::benchmark::State& state) override {
    NUMBER_OF_BYTES = state.range(0) * MB;
    NUMBER_OF_ELEMENTS = NUMBER_OF_BYTES / sizeof(uint32_t);
    data_to_write = generate_random_indexes(NUMBER_OF_ELEMENTS);
    control_sum = std::accumulate(data_to_write.begin(), data_to_write.end(), uint64_t{0});

    if (creat(filename, O_RDWR) < 1) {
      Fail("Create error:" + std::strerror(errno));
    }
    chmod(filename, S_IRUSR | S_IWUSR);  // enables owner to read and write file
  }

  void TearDown(::benchmark::State& /*state*/) override {
    Assert(std::remove(filename) == 0, "Remove error: " + std::strerror(errno));
  }

 protected:
  void sanity_check();

  std::vector<uint32_t> data_to_write;
  uint64_t control_sum = uint64_t{0};
  const char* filename = "file.txt";  //const char* needed for C-System Calls
  uint32_t NUMBER_OF_BYTES = uint32_t{0};
  uint32_t NUMBER_OF_ELEMENTS = uint32_t{0};
};

void FileIOWriteMicroBenchmarkFixture::sanity_check() {
  auto fd = int32_t{};
  if ((fd = open(filename, O_RDONLY)) < 0) {
    close(fd);
    Fail("Open error:" + std::strerror(errno));
  }

  const auto file_size = lseek(fd, 0, SEEK_END);
  Assert(file_size == NUMBER_OF_BYTES, "Sanity check failed: Actual size of " + std::to_string(file_size) +
                                           " does not match expected file size of " + std::to_string(NUMBER_OF_BYTES) +
                                           ".");

  auto read_data = std::vector<uint32_t>(NUMBER_OF_ELEMENTS);

  const off_t OFFSET = 0;
  auto* map = reinterpret_cast<uint32_t*>(mmap(NULL, NUMBER_OF_BYTES, PROT_READ, MAP_PRIVATE, fd, OFFSET));
  close(fd);

  Assert(map != MAP_FAILED, "Mapping for Sanity Check Failed:" + std::strerror(errno));

  memcpy(std::data(read_data), map, NUMBER_OF_BYTES);
  const auto sum = std::accumulate(read_data.begin(), read_data.end(), uint64_t{0});
  Assert(control_sum == sum,
         "Sanity check failed. Got: " + std::to_string(sum) + " Expected: " + std::to_string(control_sum));

  // Remove memory mapping after job is done.
  Assert(munmap(map, NUMBER_OF_BYTES) == 0, "Unmapping for Sanity Check failed: " + std::strerror(errno));
}

}  // namespace hyrise
