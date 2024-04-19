#include "shuffle.hpp"

#include <cmath>
#include <deque>
#include <memory>
#include <thread>
#include <tuple>
#include <semaphore>
#include <vector>

#include "hyrise.hpp"
#include "operators/abstract_operator.hpp"
#include "operators/progressive/chunk_sink.hpp"
#include "operators/table_wrapper.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/job_task.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/task_queue.hpp"
#include "storage/segment_iterate.hpp"
#include "storage/table.hpp"
#include "utils/assert.hpp"
#include "utils/progressive_utils.hpp"
#include "utils/timer.hpp"

namespace {

using namespace hyrise;  // NOLINT(build/namespaces)

// Each partition (deque) is described by pairs of <ColumnID, PartitionValue (a.k.a. radix)>.
using TablePartition = std::pair<std::vector<std::pair<ColumnID, size_t>>, std::deque<RowID>>;

// First bool: partition is final; second bool: partition has been merge; third is actual partitioned data.
using IntermediateShufflePartition = std::tuple<bool, bool, std::vector<TablePartition>>;

void _shuffle_chunk(const std::shared_ptr<Chunk>& chunk, auto& intermediate_results, const ColumnID column_id,
                    const size_t partition_count) {
  const auto& partition_segment = chunk->get_segment(column_id);
  const auto mask = (1 << static_cast<size_t>(std::log2(partition_count))) - 1;

  resolve_data_type(partition_segment->data_type(), [&](const auto data_type) {
    using ColumnDataType = typename decltype(data_type)::type;
    segment_iterate<ColumnDataType>(*partition_segment, [&](const auto& position) {
      std::cerr << std::format("Radix of {} is {}\n", position.value(), mask & std::hash<ColumnDataType>{}(position.value()));
    });
  });
}

void shuffle_chunk(const std::shared_ptr<Chunk>& chunk, auto& intermediate_results,
                   const std::vector<ColumnID>& columns, const std::vector<size_t>& partition_counts) {

  for (auto partition_id = size_t{0}; partition_id < columns.size(); ++partition_id) {
    _shuffle_chunk(chunk, intermediate_results, columns[partition_id], partition_counts[partition_id]);
  }
}

void merge_intermediate_results(auto& intermediate_results, auto& final_results) {}

}  // namespace

namespace hyrise {

Shuffle::Shuffle(const std::shared_ptr<const AbstractOperator>& input_operator,
                 std::shared_ptr<ChunkSink>& input_chunk_sink, std::shared_ptr<ChunkSink>& output_chunk_sink,
                 std::vector<ColumnID>&& columns, std::vector<size_t>&& partition_counts)
    : AbstractReadOnlyOperator(OperatorType::Shuffle, input_operator, nullptr),
      _input_chunk_sink{input_chunk_sink},
      _output_chunk_sink{output_chunk_sink},
      _columns{columns},
      _partition_counts{partition_counts} {
  Assert(_input_chunk_sink && _output_chunk_sink, "Shuffle requires sinks at both ends.");
  Assert(_columns.size() == _partition_counts.size(), "Unexpected partitioning configuration.");

  for (const auto& partition_count : _partition_counts) {
    Assert(std::log2(partition_count) == std::ceil(std::log2(partition_count)), "Partition sizes must be a power of two.");
  }
}

const std::string& Shuffle::name() const {
  static const auto name = std::string{"Shuffle"};
  return name;
}

std::shared_ptr<AbstractOperator> Shuffle::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_left_input,
    const std::shared_ptr<AbstractOperator>& /*copied_right_input*/,
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& /*copied_ops*/) const {
  auto copy_columns = _columns;
  auto copy_partition_counts = _partition_counts;
  auto copy =
      std::make_shared<Shuffle>(copied_left_input, _input_chunk_sink, _output_chunk_sink, std::move(copy_columns), std::move(copy_partition_counts));
  return copy;
}

void Shuffle::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

std::shared_ptr<const Table> Shuffle::_on_execute() {
  auto timer = Timer{};
  const auto& input_table = left_input_table();

  // Merge semaphore: only thread at a time should merge intermediate results.
  auto merge_semaphore = std::binary_semaphore{1};

  auto partition_count = 1;
  for (const auto& p_count : _partition_counts) {
    partition_count *= p_count;
  }
  std::cerr << "Shuffle partitions into " << partition_count << " partitions.\n";

  auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  auto unsuccessful_pulls = uint32_t{1};

  // Per chunk, we collect a vector of deques (size is number of partitions)
  auto intermediate_results = std::deque<IntermediateShufflePartition>{};
  auto final_results = std::vector<std::vector<RowID>>(partition_count);

  const auto node_queue_scheduler = std::dynamic_pointer_cast<NodeQueueScheduler>(Hyrise::get().scheduler());
  Assert(node_queue_scheduler, "NodeQueueScheduler not set?");
  Assert(node_queue_scheduler->queues().size() == 1, "Unexpected NUMA topology.");
  while (!_input_chunk_sink->finished()) {
    const auto chunk = _input_chunk_sink->pull_chunk();
    if (!chunk) {
      if (!_input_chunk_sink->finished()) {
        unsuccessful_pulls *= 2;
        // const auto sleep_ms = std::chrono::milliseconds{std::min(uint32_t{3'000}, unsuccessful_pulls / 1000)};
        const auto sleep_ms = std::chrono::milliseconds{unsuccessful_pulls / 1000};
        std::this_thread::sleep_for(sleep_ms);
        if (unsuccessful_pulls > 1'000'000) {
          std::cerr << "Error: Returning dummy empty due to timing out.\n";
          _output_chunk_sink->set_all_chunks_added();
          return Table::create_dummy_table(input_table->column_definitions());
        }

        if (unsuccessful_pulls > 100'000) {
          std::cerr << std::format(
              "Shuffle sleeps for {} ms. Input sink has {} chunks and status of finished is: {} (queue length is "
              "{}).\n",
              sleep_ms, _input_chunk_sink->chunk_count(), _input_chunk_sink->finished(),
              node_queue_scheduler->queues()[0]->estimate_load());
        }
        continue;
      }

      // All chunks processed. We're done.
      break;
    }
    unsuccessful_pulls = 1;

    jobs.emplace_back(std::make_shared<JobTask>(
        [&, chunk]() {
          shuffle_chunk(chunk, intermediate_results, _columns, _partition_counts);

          if (merge_semaphore.try_acquire()) {
            merge_intermediate_results(intermediate_results, final_results);
            merge_semaphore.release();
          }
        },
        SchedulePriority::Default));
    jobs.back()->schedule();
  }

  Hyrise::get().scheduler()->wait_for_tasks(jobs);

  Assert(_input_chunk_sink->_chunks_added == _input_chunk_sink->_chunks_pulled, "error #1");
  Assert(_input_chunk_sink->_chunks_pulled == _output_chunk_sink->_chunks_added, "error #2");
  Assert(!_input_chunk_sink->pull_chunk(), "error #3: Able to pull a job even though sink is finished.");

  merge_semaphore.acquire();
  merge_intermediate_results(intermediate_results, final_results);

  _output_chunk_sink->set_all_chunks_added();

  return Table::create_dummy_table(input_table->column_definitions());
}

}  // namespace hyrise
