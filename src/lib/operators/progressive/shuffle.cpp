#include "shuffle.hpp"

#include <deque>
#include <memory>
#include <thread>
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

namespace hyrise {

Shuffle::Shuffle(const std::shared_ptr<const AbstractOperator>& input_operator,
                 std::shared_ptr<ChunkSink>& input_chunk_sink, std::shared_ptr<ChunkSink>& output_chunk_sink,
                 const std::vector<ColumnID>& columns, const std::vector<size_t>& partition_counts)
    : AbstractReadOnlyOperator(OperatorType::TableScan, input_operator, nullptr),
      _input_chunk_sink{input_chunk_sink},
      _output_chunk_sink{output_chunk_sink},
      _columns{columns},
      _partition_counts{partition_counts} {
  Assert(_input_chunk_sink && _output_chunk_sink, "Shuffle requires sinks at both ends.");
}

const std::string& Shuffle::name() const {
  static const auto name = std::string{"Shuffle"};
  return name;
}

std::shared_ptr<AbstractOperator> Shuffle::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_left_input,
    const std::shared_ptr<AbstractOperator>& /*copied_right_input*/,
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& /*copied_ops*/) const {
  auto copy =
      std::make_shared<Shuffle>(copied_left_input, _input_chunk_sink, _output_chunk_sink, _columns, _partition_counts);
  return copy;
}

void Shuffle::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

std::shared_ptr<const Table> Shuffle::_on_execute() {
  auto timer = Timer{};
  const auto& input_table = left_input_table();

  auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  auto unsuccessful_pulls = uint32_t{1};

  // Per chunk, we collect a vector of deques (size is number of partitions)

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

        },
        SchedulePriority::Default));
    jobs.back()->schedule();
  }

  Hyrise::get().scheduler()->wait_for_tasks(jobs);
  Assert(_input_chunk_sink->_chunks_added == _input_chunk_sink->_chunks_pulled, "error #1");
  Assert(_input_chunk_sink->_chunks_pulled == _output_chunk_sink->_chunks_added, "error #2");
  Assert(!_input_chunk_sink->pull_chunk(), "error #3: Able to pull a job even though sink is finished.");

  _output_chunk_sink->set_all_chunks_added();

  return Table::create_dummy_table(input_table->column_definitions());
}

}  // namespace hyrise
