#include <memory>
#include <vector>

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
  EXPECT_THROW(batch_chunks_for_scheduling(empty_table, [&](auto, auto) {}), std::logic_error);
}

TEST_F(SchedulingUtilsTest, SingleThreadedGrouping) {
  constexpr auto CHUNK_COUNT = size_t{20};
  const auto table_generator = std::make_shared<SyntheticTableGenerator>();
  const auto table = table_generator->generate_table(1, CHUNK_COUNT * 2, ChunkOffset{2});
  EXPECT_EQ(table->chunk_count(), CHUNK_COUNT);

  if constexpr (HYRISE_DEBUG) {
    const auto [jobs, chunk_ids] = batch_chunks_for_scheduling(table, [&](auto, auto) {});
    EXPECT_EQ(jobs.size(), CHUNK_COUNT);
    EXPECT_EQ(chunk_ids.size(), table->chunk_count());
  } else {
    const auto [jobs, chunk_ids] = batch_chunks_for_scheduling(table, [&](auto, auto) {});
    EXPECT_EQ(jobs.size(), 1);
    EXPECT_EQ(chunk_ids.size(), table->chunk_count());
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
  auto group_markers = std::vector<size_t>{};
  auto chunk_markers = std::vector<size_t>(CHUNK_COUNT);
  const auto [jobs, chunk_ids] = batch_chunks_for_scheduling(table, [&](const auto group_id, auto&& chunks) {
    ++sum;
    EXPECT_EQ(CHUNK_COUNT / THREAD_COUNT, chunks.size());

    for (const auto chunk_id : chunks) {
      chunk_markers[chunk_id] = chunk_id;
    }

    ASSERT_FALSE(group_markers.empty());
    // As we resize to the returned group size before spawning the jobs, this write should be safe.
    group_markers[group_id] = group_id;
  });
  const auto group_count = jobs.size();
  group_markers.resize(group_count);
  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);

  EXPECT_EQ(chunk_ids.size(), table->chunk_count());
  EXPECT_EQ(group_count, THREAD_COUNT);
  EXPECT_EQ(group_count, sum);
  for (auto marker_index = size_t{0}; marker_index < group_markers.size(); ++marker_index) {
    EXPECT_EQ(group_markers[marker_index], marker_index);
  }
  for (auto chunk_index = size_t{0}; chunk_index < chunk_markers.size(); ++chunk_index) {
    EXPECT_EQ(chunk_markers[chunk_index], chunk_index);
  }
}

}  // namespace hyrise
