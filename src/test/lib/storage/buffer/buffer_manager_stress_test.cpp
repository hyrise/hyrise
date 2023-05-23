#include <algorithm>
#include <future>
#include <numeric>
#include <random>
#include <thread>
#include "base_test.hpp"
#include "hyrise.hpp"
#include "scheduler/node_queue_scheduler.hpp"

namespace hyrise {

class BufferManagerStressTest : public BaseTest {
 protected:
  void SetUp() override {
    Hyrise::reset();
  }
};

TEST_F(BufferManagerStressTest, TestVariousAllocationsReadWrites) {
  Hyrise::get().scheduler()->wait_for_all_tasks();
  const auto previous_scheduler = Hyrise::get().scheduler();
  Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());
  std::vector<std::shared_ptr<AbstractTask>> jobs;

  constexpr auto num_vectors = 100;

  std::vector<int> vector_sizes(num_vectors);
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dis(bytes_for_size_type(MIN_PAGE_SIZE_TYPE) / sizeof(int),
                                      bytes_for_size_type(MAX_PAGE_SIZE_TYPE) / sizeof(int));
  std::generate(vector_sizes.begin(), vector_sizes.end(), [&]() { return dis(gen); });

  // Create various pmr_vectors of different sizes, creation, shuffling, sorting and verification happens
  // in different scopes to test locking mechanisms
  for (int i = 0; i < num_vectors; i++) {
    jobs.emplace_back(std::make_shared<JobTask>([&vector_sizes, i]() {
      pmr_vector<int> vector = [&vector_sizes, i]() {
        auto allocator = PolymorphicAllocator<int>{get_buffer_manager_memory_resource()};
        auto pin_guard = AllocatorPinGuard{allocator};
        pmr_vector<int> vector(vector_sizes[i], allocator);
        std::iota(vector.begin(), vector.end(), i);
        std::reverse(vector.begin(), vector.end());
        return std::move(vector);
      }();

      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      {
        ReadPinGuard guard{vector};
        EXPECT_FALSE(std::is_sorted(vector.begin(), vector.end()));
      }
      {
        WritePinGuard guard{vector};
        std::sort(vector.begin(), vector.end());
      }
      {
        ReadPinGuard guard{vector};

        EXPECT_EQ(*vector.begin(), i);
        EXPECT_EQ(*(vector.end() - 1), vector_sizes[i] + i - 1);

        EXPECT_TRUE(std::is_sorted(vector.begin(), vector.end()));
      }
    }));

    jobs.back()->schedule();
  }

  Hyrise::get().scheduler()->wait_for_tasks(jobs);
}

}  // namespace hyrise
