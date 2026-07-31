#include <cstdint>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "base_test.hpp"
#include "hyrise.hpp"
#include "operators/aggregate_dyod_utils/concurrent_ticket_map.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/job_task.hpp"
#include "scheduler/node_queue_scheduler.hpp"

namespace hyrise {

class ConcurrentTicketMapTest : public BaseTest {};

namespace {

struct MurmurHash {
  size_t operator()(const uint64_t value) const {
    return compute_hash(&value, sizeof(value));
  }
};

using TestMap = ConcurrentTicketMap<uint64_t, MurmurHash>;

std::vector<std::pair<uint64_t, uint64_t>> extract(const TestMap& map) {
  auto entries = std::vector<std::pair<uint64_t, uint64_t>>{};
  map.for_each([&](const uint64_t key, const uint64_t ticket) {
    entries.emplace_back(key, ticket);
  });

  return entries;
}

std::unordered_map<uint64_t, uint64_t> extract_unique(const TestMap& map) {
  const auto entries = extract(map);
  const auto key_to_ticket = std::unordered_map<uint64_t, uint64_t>{entries.begin(), entries.end()};
  EXPECT_EQ(key_to_ticket.size(), entries.size());

  return key_to_ticket;
}

constexpr auto KEY_COUNT = uint64_t{10'000};
constexpr auto JOB_COUNT = uint64_t{8};

}  // namespace

TEST_F(ConcurrentTicketMapTest, KeyKeepsTicket) {
  auto map = TestMap{KEY_COUNT};
  map.register_prober();

  for (auto key = uint64_t{0}; key < KEY_COUNT; ++key) {
    EXPECT_EQ(map.try_emplace(key, key), key);
  }

  // A second pass must hand back the original tickets and ignore the 'proposed' ones.
  for (auto key = uint64_t{0}; key < KEY_COUNT; ++key) {
    EXPECT_EQ(map.try_emplace(key, KEY_COUNT + key), key);
  }

  EXPECT_EQ(extract(map).size(), KEY_COUNT);

  map.unregister_prober();
}

TEST_F(ConcurrentTicketMapTest, ResizeKeepsEntriesAndLookups) {
  auto map = TestMap{KEY_COUNT};
  map.register_prober();

  for (auto key = uint64_t{0}; key < KEY_COUNT; ++key) {
    map.try_emplace(key, key);
  }

  map.resize(KEY_COUNT * 4);

  const auto entries = extract_unique(map);
  ASSERT_EQ(entries.size(), KEY_COUNT);
  for (auto key = uint64_t{0}; key < KEY_COUNT; ++key) {
    EXPECT_EQ(entries.at(key), key);
    EXPECT_EQ(map.try_emplace(key, KEY_COUNT + key), key);
  }
  map.unregister_prober();
}

TEST_F(ConcurrentTicketMapTest, InsertWithBadSizeEstimate) {
  // We only reserve 1/10 of the keys, so during the insertions the map will have to grow.
  auto map = TestMap{KEY_COUNT / 10};
  map.register_prober();

  for (auto key = uint64_t{0}; key < KEY_COUNT; ++key) {
    EXPECT_EQ(map.try_emplace(key, key), key);
  }

  const auto entries = extract_unique(map);
  ASSERT_EQ(entries.size(), KEY_COUNT);
  for (auto key = uint64_t{0}; key < KEY_COUNT; ++key) {
    EXPECT_EQ(entries.at(key), key);
    EXPECT_EQ(map.try_emplace(key, KEY_COUNT + key), key);
  }
  map.unregister_prober();
}

TEST_F(ConcurrentTicketMapTest, ConcurrentInsertsAssignOneTicketPerKey) {
  Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());

  auto map = TestMap{KEY_COUNT};
  auto returned_tickets = std::vector<std::vector<uint64_t>>(JOB_COUNT, std::vector<uint64_t>(KEY_COUNT));

  // Every job inserts the full range, so all but one job see each key as already present.
  auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  jobs.reserve(JOB_COUNT);
  for (auto job_id = uint64_t{0}; job_id < JOB_COUNT; ++job_id) {
    jobs.emplace_back(std::make_shared<JobTask>([&, job_id] {
      map.register_prober();
      auto next_ticket = job_id * KEY_COUNT;
      for (auto key = uint64_t{0}; key < KEY_COUNT; ++key) {
        const auto ticket = map.try_emplace(key, next_ticket);
        returned_tickets[job_id][key] = ticket;
        next_ticket += ticket == next_ticket ? 1 : 0;
      }
      map.unregister_prober();
    }));
  }
  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);

  map.register_prober();
  const auto entries = extract_unique(map);
  ASSERT_EQ(entries.size(), KEY_COUNT);

  // All jobs must agree on the ticket of a key, no matter which one won the race.
  auto distinct_tickets = std::unordered_set<uint64_t>{};
  for (auto key = uint64_t{0}; key < KEY_COUNT; ++key) {
    const auto ticket = entries.at(key);
    EXPECT_TRUE(distinct_tickets.insert(ticket).second);

    for (auto job_id = uint64_t{0}; job_id < JOB_COUNT; ++job_id) {
      EXPECT_EQ(returned_tickets[job_id][key], ticket);
    }
  }
  map.unregister_prober();
}

}  // namespace hyrise
