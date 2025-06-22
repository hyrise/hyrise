#include <gtest/gtest.h>
#include <numa.h>
#include <unistd.h>

#include <unordered_set>

#include "memory/linear_numa_memory_resource.hpp"
#include "memory.hpp"

// Release assertions boiler plate //////////////

[[noreturn]] inline void fail(const std::string& msg) {
  throw std::logic_error(msg);
}

#define Fail(msg) \
  fail(msg);      \
  static_assert(true, "End call of macro with a semicolon.")

#define Assert(expr, msg)         \
  if (!static_cast<bool>(expr)) { \
    Fail(msg);                    \
  }                               \
  static_assert(true, "End call of macro with a semicolon.")

/////////////////////////////////////////

namespace memory {

class LinearNumaMemoryResourceTest : public ::testing::Test {
 protected:
  void SetUp() override {
    numa_init();
    valid_node_ids = allowed_memory_node_ids();
  }
  NumaNodeIDs valid_node_ids;
};

TEST_F(LinearNumaMemoryResourceTest, Allocate) {
  constexpr auto size = 1024u * 1024u * 1024u;  // 1 GiB
  auto resource = hyrise::LinearNumaMemoryResource(size, {});
  constexpr auto alloc_size = 4096u;
  constexpr auto safe_alloc_count = 262144u - 1;

  auto addresses = std::unordered_set<void*>{};
  addresses.reserve(safe_alloc_count);
  for (auto alloc_count = 0u; alloc_count < safe_alloc_count; ++alloc_count) {
    SCOPED_TRACE("Alloc count: " + std::to_string(alloc_count));
    auto addr = resource.allocate(alloc_size, 8u);
    auto [_, inserted] = addresses.insert(addr);
    ASSERT_TRUE(inserted);
  }
  for (const auto& addr : addresses) {
    ASSERT_TRUE(resource.address_in_range(addr));
  }

  {
    auto different_resource = hyrise::LinearNumaMemoryResource(1024u, {});
    const auto addr_different_resource = different_resource.allocate(8u, 8u);
    ASSERT_FALSE(resource.address_in_range(addr_different_resource));
  }

  EXPECT_THROW(auto next_addr = resource.allocate(alloc_size * 2, 8u), std::logic_error);
}

TEST_F(LinearNumaMemoryResourceTest, NodePinningRoundRobin) {
  if (valid_node_ids.size() < 2) {
    GTEST_SKIP() << "Skipping test: system has " << valid_node_ids.size() << " but test requires at least 2.";
  }
  constexpr auto BUFFER_SIZE = 1024u * 128;
  const auto numa_nodes = NumaNodeIDs{valid_node_ids[0], valid_node_ids[1]};
  const auto resource = hyrise::LinearNumaMemoryResource(BUFFER_SIZE, numa_nodes);

  auto* buffer_start = resource.data();
  auto target_node_iter = NumaNodeRingIterator(valid_node_ids);
  auto page_statuses = get_page_statuses(buffer_start, BUFFER_SIZE);

  for (auto page_id = 0; auto& page_status : page_statuses) {
    ASSERT_EQ(page_status, target_node_iter.next(page_status));
    ++page_id;
  }
}

TEST_F(LinearNumaMemoryResourceTest, NodePinningRoundRobinLarge) {
  if (valid_node_ids.size() < 2) {
    GTEST_SKIP() << "Skipping test: system has " << valid_node_ids.size() << " but test requires at least 2.";
  }
  constexpr auto BUFFER_SIZE = 1024u * 1024 * 1024 * 10;
  const auto numa_nodes = NumaNodeIDs{valid_node_ids[0], valid_node_ids[1]};
  const auto resource = hyrise::LinearNumaMemoryResource(BUFFER_SIZE, numa_nodes);

  auto* buffer_start = resource.data();
  auto target_node_iter = NumaNodeRingIterator(valid_node_ids);
  auto page_statuses = get_page_statuses(buffer_start, BUFFER_SIZE);

  for (auto page_id = 0; auto& page_status : page_statuses) {
    ASSERT_EQ(page_status, target_node_iter.next(page_status));
    ++page_id;
  }
}

TEST_F(LinearNumaMemoryResourceTest, NodePinningWeighted) {
  if (valid_node_ids.size() < 2) {
    GTEST_SKIP() << "Skipping test: system has " << valid_node_ids.size() << " but test requires at least 2.";
  }
  constexpr auto BUFFER_SIZE = 1024u * 128;
  const auto numa_nodes = NumaNodeIDs{valid_node_ids[0], valid_node_ids[1]};
  const auto node_weights = InterleavingWeights{2,5};
  const auto resource = hyrise::LinearNumaMemoryResource(BUFFER_SIZE, numa_nodes, node_weights);

  auto* buffer_start = resource.data();
  auto page_statuses = get_page_statuses(buffer_start, BUFFER_SIZE);

  auto expected_page_locations = PageLocations{};
  hyrise::LinearNumaMemoryResource::fill_page_locations_weighted_interleaved(expected_page_locations, BUFFER_SIZE, numa_nodes,
    node_weights);
  for (auto page_id = 0; auto& page_status : page_statuses) {
    ASSERT_EQ(page_status, expected_page_locations[page_id]);
    // printf("actual: %i, expected: %i\n", page_status, expected_page_locations[page_id]);
    ++page_id;
  }
}

TEST_F(LinearNumaMemoryResourceTest, NodePinningWeightedLarge) {
  if (valid_node_ids.size() < 2) {
    GTEST_SKIP() << "Skipping test: system has " << valid_node_ids.size() << " but test requires at least 2.";
  }
  constexpr auto BUFFER_SIZE = 1024u * 1024 * 1024 * 10;
  const auto numa_nodes = NumaNodeIDs{valid_node_ids[0], valid_node_ids[1]};
  const auto node_weights = InterleavingWeights{2,5};
  const auto resource = hyrise::LinearNumaMemoryResource(BUFFER_SIZE, numa_nodes, node_weights);

  auto* buffer_start = resource.data();
  auto page_statuses = get_page_statuses(buffer_start, BUFFER_SIZE);

  auto expected_page_locations = PageLocations{};
  hyrise::LinearNumaMemoryResource::fill_page_locations_weighted_interleaved(expected_page_locations, BUFFER_SIZE, numa_nodes,
    node_weights);
  for (auto page_id = 0; auto& page_status : page_statuses) {
    ASSERT_EQ(page_status, expected_page_locations[page_id]);
    // printf("actual: %i, expected: %i\n", page_status, expected_page_locations[page_id]);
    ++page_id;
  }
}

}  // namespace hyrise

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
