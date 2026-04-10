#include "base_test.hpp"
#include "hyrise.hpp"
#include "operators/table_wrapper.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "synthetic_table_generator.hpp"
#include "types.hpp"
#include "utils/scheduling_utils.hpp"

namespace hyrise {

class SchedulingUtilsTest : public BaseTest {};

TEST_F(SchedulingUtilsTest, NoChunksTableGrouping) {
  const auto empty_table = Table::create_dummy_table({{"a", DataType::Int, false}});
  EXPECT_THROW(group_chunks_for_scheduling(empty_table, [&](auto, auto) {}), std::logic_error);
}

TEST_F(SchedulingUtilsTest, SingleThreadedGrouping) {
  constexpr auto CHUNK_COUNT = size_t{20};
  const auto table_generator = std::make_shared<SyntheticTableGenerator>();
  const auto table = table_generator->generate_table(1, CHUNK_COUNT * 2, ChunkOffset{2});
  EXPECT_EQ(table->chunk_count(), CHUNK_COUNT);

  if constexpr (HYRISE_DEBUG) {
    const auto jobs = group_chunks_for_scheduling(table, [&](auto, auto) {});
    EXPECT_EQ(jobs.size(), CHUNK_COUNT);
  } else {
    const auto jobs = group_chunks_for_scheduling(table, [&](auto, auto) {});
    EXPECT_EQ(jobs.size(), 1);
  }
}

TEST_F(SchedulingUtilsTest, MultiThreadedGrouping) {
  constexpr auto THREAD_COUNT = size_t{4};
  Hyrise::get().topology.use_fake_numa_topology(THREAD_COUNT, THREAD_COUNT);
  Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());

  constexpr auto CHUNK_COUNT = size_t{20};
  const auto table_generator = std::make_shared<SyntheticTableGenerator>();
  const auto table = table_generator->generate_table(1, CHUNK_COUNT * 2, ChunkOffset{2});
  EXPECT_EQ(table->chunk_count(), CHUNK_COUNT);

  auto sum = std::atomic<size_t>{0};
  auto markers = std::vector<size_t>{};
  const auto jobs = group_chunks_for_scheduling(table, [&](const auto group_id, const auto chunks) {
    ++sum;
    EXPECT_EQ(CHUNK_COUNT / THREAD_COUNT, chunks->size());

    // As we resize to the returned group size before spawning the jobs, this write should be safe.
    markers[group_id] = 17;
  });
  const auto group_count = jobs.size();
  markers.resize(group_count);
  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);

  EXPECT_EQ(group_count, THREAD_COUNT);
  EXPECT_EQ(group_count, sum);
  for (const auto& marker : markers) {
    EXPECT_EQ(marker, 17);
  }

  Hyrise::get().scheduler()->finish();
}

}  // namespace hyrise
