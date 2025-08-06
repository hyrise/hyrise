#include <algorithm>
#include <fstream>
#include <iostream>
#include <random>

// This playground only compiles on Linux as we require Linux's perf and perfetto.
#include "hyrise.hpp"
#include "perfcpp/event_counter.h"
#include "perfetto.h"
#include "scheduler/job_task.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "storage/chunk.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

using namespace hyrise;  // NOLINT(build/namespaces)

constexpr auto STRING_COUNT = size_t{100'000'000};  // Careful: 100M has an RSS of ~30GB.

// Define trace categories
PERFETTO_DEFINE_CATEGORIES(perfetto::Category("Sort").SetDescription("Benchmark parallel sorting"));

PERFETTO_TRACK_EVENT_STATIC_STORAGE();

std::vector<std::string> get_top_by_stdsort(const size_t k, std::vector<std::string>& input) {
  std::ranges::sort(input);
  return std::vector<std::string>(input.begin(), input.begin() + k);
}

std::vector<std::string> get_top_by_indexsort(const size_t k, std::vector<std::string>& input) {
  static_assert(std::numeric_limits<uint32_t>::max() >= STRING_COUNT);
  auto offsets = std::vector<uint32_t>(input.size());
  std::iota(offsets.begin(), offsets.end(), 0);

  std::ranges::sort(offsets, [&](const auto& lhs, const auto& rhs) {
    return input[lhs] < input[rhs];
  });

  auto result = std::vector<std::string>{};
  result.reserve(k);

  for (auto index = size_t{0}; index < k; ++index) {
    result.push_back(input[offsets[index]]);
  }

  return result;
}

std::vector<std::string> get_top_by_priorityqueue(const size_t k, std::vector<std::string>& input) {
  // We use a max queue to mimic a fixed-size min-queue.
  auto priority_queue = std::priority_queue<std::string>{};

  for (const auto& string : input) {
    if (priority_queue.size() < k) {
      priority_queue.push(string);
      continue;
    }

    if (priority_queue.top() > string) {
      priority_queue.pop();
      priority_queue.push(string);
    }
  }

  Assert(priority_queue.size() == k, "Unexpected size of priority queue.");
  auto result = std::vector<std::string>(k);
  for (auto index = k; index > 0; --index) {
    result[index - 1] = priority_queue.top();
    priority_queue.pop();
  }

  return result;
}

template <typename Iterator>
void merge_sort(Iterator first, Iterator last) {
  TRACE_EVENT("Sort", "MergeSort");
  if (std::distance(first, last) <= Chunk::DEFAULT_SIZE) {
    TRACE_EVENT("Sort", "MergeSort::sort");
    std::sort(first, last);
    return;
  }

  auto middle = first + (std::distance(first, last) / 2);
  auto tasks = std::vector<std::shared_ptr<AbstractTask>>{};
  tasks.emplace_back(std::make_shared<JobTask>([&]() {
    merge_sort(first, middle);
  }));
  tasks.emplace_back(std::make_shared<JobTask>([&]() {
    merge_sort(middle, last);
  }));

  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(tasks);

  TRACE_EVENT("Sort", "MergeSort::merge");
  std::inplace_merge(first, middle, last);
}

void parallel_merge_sort(std::vector<std::string>& input) {
  Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());

  TRACE_EVENT("Sort", "ParallelMergeSort");
  merge_sort(input.begin(), input.end());

  if (!std::ranges::is_sorted(input)) {
    std::cerr << "Input not sorted.\n";
  }

  Hyrise::get().scheduler()->finish();
}

std::vector<std::string> get_std_strings(auto& generator, auto& random_distribution) {
  auto std_strings = std::vector<std::string>(STRING_COUNT);
  for (auto index = size_t{0}; index < STRING_COUNT; ++index) {
    // Should always exceed SSO. Expensive comparisons.
    std_strings[index] =
        std::string{"2042-02-31 10:10:10_"} + std::to_string(random_distribution(generator)) + "_remainderstring";
  }

  std::ranges::shuffle(std_strings, generator);

  return std_strings;
}

int main() {
  auto generator = std::mt19937{17};
  auto random_distribution = std::uniform_int_distribution<uint32_t>();

  auto base_data = get_std_strings(generator, random_distribution);
  auto compare_result_data_copy = base_data;
  const auto compare_result = get_top_by_stdsort(100, compare_result_data_copy);

  /**
   * The following usage of perf-cpp and Perfetto is just to show case how to use these tools. Feel free to add helper
   * methods, classes, ... whatever you need.
   *
   * Initialize the perf-cpp counters.
   */
  auto counters = perf::CounterDefinition{};
  auto event_counter = perf::EventCounter{counters};

  // Specify hardware events to count.
  event_counter.add(
      {"seconds", "instructions", "cycles", "cache-misses", "dTLB-miss-ratio"});  // Not possible on the VM server.
  // event_counter.add("seconds");  // Runs on the VM server. Not too helpful though.

  const auto functions = std::vector<
      std::pair<std::string, std::function<std::vector<std::string>(const size_t, std::vector<std::string>&)>>>{
      {"get_top_by_stdsort", get_top_by_stdsort},
      {"get_top_by_indexsort", get_top_by_indexsort},
      {"get_top_by_priorityqueue", get_top_by_priorityqueue}};

  for (const auto& [function_name, function] : functions) {
    auto benchmark_data = base_data;

    // Run the workload and track with perf-cpp.
    event_counter.start();
    const auto result = function(100, benchmark_data);
    event_counter.stop();

    Assert(result.size() == compare_result.size(), "Wrong result size.");
    for (auto index = size_t{0}; index < compare_result.size(); ++index) {
      Assert(result[index] == compare_result[index], "Wrong item at position " + std::to_string(index) + ".");
    }

    // Print the results.
    std::cout << ">>> " << function_name << '\n';
    const auto perf_result = event_counter.result();
    for (const auto& [event_name, value] : perf_result) {
      std::cout << event_name << ": " << value << '\n';
    }
    std::cout << '\n';
  }

  /**
   * Initialize Perfetto.
   */
  auto track_event_cfg = perfetto::protos::gen::TrackEventConfig{};
  track_event_cfg.add_enabled_categories("Sort");

  auto args = perfetto::TracingInitArgs{};
  args.backends = perfetto::kInProcessBackend;
  perfetto::Tracing::Initialize(args);
  perfetto::TrackEvent::Register();

  auto cfg = perfetto::TraceConfig{};
  cfg.add_buffers()->set_size_kb(4096);
  auto* ds_cfg = cfg.add_data_sources()->mutable_config();
  ds_cfg->set_name("track_event");
  ds_cfg->set_track_event_config_raw(track_event_cfg.SerializeAsString());

  auto tracing_session = std::unique_ptr<perfetto::TracingSession>(perfetto::Tracing::NewTrace());
  tracing_session->Setup(cfg);
  tracing_session->StartBlocking();

  auto sort_data = base_data;
  parallel_merge_sort(sort_data);

  tracing_session->StopBlocking();
  auto trace_data = std::vector<char>(tracing_session->ReadTraceBlocking());

  auto output = std::ofstream{};
  output.open("dyod2025_mergesort.perfetto-trace", std::ios::out | std::ios::binary);
  output.write(&trace_data[0], trace_data.size());
  output.close();

  return 0;
}